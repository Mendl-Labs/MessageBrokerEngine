// CPU affinity and thread optimization for ultra-low latency
// Implements CPU pinning recommendations from latency profiling

use std::thread;

#[cfg(target_os = "linux")]
use libc::{cpu_set_t, sched_setaffinity, CPU_SET, CPU_ZERO};

#[cfg(target_os = "windows")]
extern "system" {
    fn GetCurrentThread() -> *mut std::ffi::c_void;
    fn SetThreadAffinityMask(hThread: *mut std::ffi::c_void, dwThreadAffinityMask: usize) -> usize;
    fn GetSystemInfo(lpSystemInfo: *mut SYSTEM_INFO);
}

#[cfg(target_os = "windows")]
#[repr(C)]
struct SYSTEM_INFO {
    processor_architecture: u16,
    reserved: u16,
    page_size: u32,
    minimum_application_address: *mut std::ffi::c_void,
    maximum_application_address: *mut std::ffi::c_void,
    active_processor_mask: usize,
    number_of_processors: u32,
    processor_type: u32,
    allocation_granularity: u32,
    processor_level: u16,
    processor_revision: u16,
}

/// CPU affinity configuration for optimal performance
#[derive(Debug, Clone)]
pub struct CpuAffinityConfig {
    /// Dedicated CPU cores for publisher threads
    pub publisher_cores: Vec<usize>,
    /// Dedicated CPU cores for network I/O
    pub network_cores: Vec<usize>,
    /// Enable CPU isolation (avoid OS scheduler interference)
    pub enable_isolation: bool,
    /// Thread priority (1-99, higher = more priority)
    pub thread_priority: u8,
}

impl Default for CpuAffinityConfig {
    fn default() -> Self {
        Self {
            publisher_cores: vec![2, 3], // Use cores 2-3 for publishing
            network_cores: vec![4, 5],   // Use cores 4-5 for network I/O
            enable_isolation: true,
            thread_priority: 90,         // High priority
        }
    }
}

/// CPU optimization utilities
pub struct CpuOptimizer {
    config: CpuAffinityConfig,
    available_cores: usize,
}

impl CpuOptimizer {
    pub fn new(config: CpuAffinityConfig) -> Self {
        let available_cores = Self::get_available_cores();
        
        Self {
            config,
            available_cores,
        }
    }
    
    /// Get the number of available CPU cores
    pub fn get_available_cores() -> usize {
        #[cfg(target_os = "linux")]
        {
            std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(1)
        }
        
        #[cfg(target_os = "windows")]
        {
            let mut system_info: SYSTEM_INFO = unsafe { std::mem::zeroed() };
            unsafe {
                GetSystemInfo(&mut system_info);
            }
            system_info.number_of_processors as usize
        }
        
        #[cfg(not(any(target_os = "linux", target_os = "windows")))]
        {
            std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(1)
        }
    }
    
    /// Pin current thread to publisher CPU cores
    pub fn pin_publisher_thread(&self) -> Result<(), CpuAffinityError> {
        self.pin_thread_to_cores(&self.config.publisher_cores)
    }
    
    /// Pin current thread to network I/O CPU cores
    pub fn pin_network_thread(&self) -> Result<(), CpuAffinityError> {
        self.pin_thread_to_cores(&self.config.network_cores)
    }
    
    /// Pin current thread to specific CPU cores
    fn pin_thread_to_cores(&self, cores: &[usize]) -> Result<(), CpuAffinityError> {
        if cores.is_empty() {
            return Err(CpuAffinityError::InvalidCores);
        }
        
        // Validate core numbers
        for &core in cores {
            if core >= self.available_cores {
                return Err(CpuAffinityError::CoreNotAvailable(core));
            }
        }
        
        #[cfg(target_os = "linux")]
        {
            self.pin_thread_linux(cores)
        }
        
        #[cfg(target_os = "windows")]
        {
            self.pin_thread_windows(cores)
        }
        
        #[cfg(not(any(target_os = "linux", target_os = "windows")))]
        {
            tracing::warn!("CPU affinity not supported on this platform");
            Ok(())
        }
    }
    
    #[cfg(target_os = "linux")]
    fn pin_thread_linux(&self, cores: &[usize]) -> Result<(), CpuAffinityError> {
        unsafe {
            let mut cpuset: cpu_set_t = std::mem::zeroed();
            CPU_ZERO(&mut cpuset);
            
            for &core in cores {
                CPU_SET(core, &mut cpuset);
            }
            
            let result = sched_setaffinity(0, std::mem::size_of::<cpu_set_t>(), &cpuset);
            if result != 0 {
                Err(CpuAffinityError::SystemError(std::io::Error::last_os_error()))
            } else {
                tracing::info!("Thread pinned to CPU cores: {:?}", cores);
                Ok(())
            }
        }
    }
    
    #[cfg(target_os = "windows")]
    fn pin_thread_windows(&self, cores: &[usize]) -> Result<(), CpuAffinityError> {
        let mut affinity_mask: usize = 0;
        
        for &core in cores {
            affinity_mask |= 1 << core;
        }
        
        unsafe {
            let result = SetThreadAffinityMask(GetCurrentThread(), affinity_mask);
            if result == 0 {
                Err(CpuAffinityError::SystemError(std::io::Error::last_os_error()))
            } else {
                tracing::info!("Thread pinned to CPU cores: {:?}", cores);
                Ok(())
            }
        }
    }
    
    /// Set thread priority for low latency
    pub fn set_high_priority(&self) -> Result<(), CpuAffinityError> {
        #[cfg(target_os = "linux")]
        {
            // On Linux, use nice() or sched_setscheduler for real-time priority
            use libc::{setpriority, PRIO_PROCESS};
            
            let priority = -(self.config.thread_priority as i32);
            unsafe {
                if setpriority(PRIO_PROCESS, 0, priority) != 0 {
                    return Err(CpuAffinityError::SystemError(std::io::Error::last_os_error()));
                }
            }
        }
        
        #[cfg(target_os = "windows")]
        {
            // On Windows, use SetThreadPriority
            use winapi::um::processthreadsapi::{GetCurrentThread, SetThreadPriority};
            use winapi::um::winbase::THREAD_PRIORITY_TIME_CRITICAL;
            
            unsafe {
                if SetThreadPriority(GetCurrentThread(), THREAD_PRIORITY_TIME_CRITICAL as i32) == 0 {
                    return Err(CpuAffinityError::SystemError(std::io::Error::last_os_error()));
                }
            }
        }
        
        tracing::info!("Thread priority set to high performance mode");
        Ok(())
    }
    
    /// Create optimized thread for publisher operations
    pub fn spawn_publisher_thread<F, T>(&self, f: F) -> Result<thread::JoinHandle<T>, CpuAffinityError>
    where
        F: FnOnce() -> T + Send + 'static,
        T: Send + 'static,
    {
        let optimizer = self.clone();
        
        let handle = thread::Builder::new()
            .name("publisher_thread".to_string())
            .spawn(move || {
                // Pin to publisher cores
                if let Err(e) = optimizer.pin_publisher_thread() {
                    tracing::warn!("Failed to pin publisher thread: {:?}", e);
                }
                
                // Set high priority
                if let Err(e) = optimizer.set_high_priority() {
                    tracing::warn!("Failed to set high priority: {:?}", e);
                }
                
                // Execute the function
                f()
            })
            .map_err(|e| CpuAffinityError::ThreadSpawnError(e))?;
        
        Ok(handle)
    }
    
    /// Create optimized thread for network I/O operations
    pub fn spawn_network_thread<F, T>(&self, f: F) -> Result<thread::JoinHandle<T>, CpuAffinityError>
    where
        F: FnOnce() -> T + Send + 'static,
        T: Send + 'static,
    {
        let optimizer = self.clone();
        
        let handle = thread::Builder::new()
            .name("network_thread".to_string())
            .spawn(move || {
                // Pin to network cores
                if let Err(e) = optimizer.pin_network_thread() {
                    tracing::warn!("Failed to pin network thread: {:?}", e);
                }
                
                // Set high priority
                if let Err(e) = optimizer.set_high_priority() {
                    tracing::warn!("Failed to set high priority: {:?}", e);
                }
                
                // Execute the function
                f()
            })
            .map_err(|e| CpuAffinityError::ThreadSpawnError(e))?;
        
        Ok(handle)
    }
    
    /// Get CPU optimization recommendations
    pub fn get_recommendations(&self) -> Vec<String> {
        let mut recommendations = Vec::new();
        
        if self.available_cores < 4 {
            recommendations.push("⚠️  Consider upgrading to a system with 4+ CPU cores for optimal performance".to_string());
        }
        
        if self.config.publisher_cores.iter().any(|&core| core >= self.available_cores) {
            recommendations.push("⚠️  Some configured CPU cores are not available".to_string());
        }
        
        if self.config.publisher_cores.iter().any(|&core| core < 2) {
            recommendations.push("💡 Avoid using CPU cores 0-1 for critical threads (often used by OS)".to_string());
        }
        
        if !self.config.enable_isolation {
            recommendations.push("💡 Enable CPU isolation for better latency consistency".to_string());
        }
        
        recommendations.push(format!("✅ Optimal configuration: {} available cores, publisher on {:?}, network on {:?}", 
            self.available_cores, self.config.publisher_cores, self.config.network_cores));
        
        recommendations
    }
}

impl Clone for CpuOptimizer {
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            available_cores: self.available_cores,
        }
    }
}

/// CPU affinity error types
#[derive(Debug, thiserror::Error)]
pub enum CpuAffinityError {
    #[error("Invalid CPU cores specified")]
    InvalidCores,
    #[error("CPU core {0} is not available")]
    CoreNotAvailable(usize),
    #[error("System error: {0}")]
    SystemError(std::io::Error),
    #[error("Thread spawn error: {0}")]
    ThreadSpawnError(std::io::Error),
}

/// Convenient macro for pinning current thread to publisher cores
#[macro_export]
macro_rules! pin_publisher_thread {
    ($optimizer:expr) => {
        if let Err(e) = $optimizer.pin_publisher_thread() {
            tracing::warn!("Failed to pin thread: {:?}", e);
        }
    };
}

/// Convenient macro for pinning current thread to network cores
#[macro_export]
macro_rules! pin_network_thread {
    ($optimizer:expr) => {
        if let Err(e) = $optimizer.pin_network_thread() {
            tracing::warn!("Failed to pin thread: {:?}", e);
        }
    };
}
