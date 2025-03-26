# 📨 Message Broker Server (Rust)

A lightweight, high-performance TCP-based message broker server written in Rust using Protobuf for communication. This server handles subscription and publication to topics over persistent TCP connections and provides graceful shutdown and concurrency management.

---

## 🚀 Features

- ⚙️ Configurable TCP listener via `appsettings.json`
- 🔐 Protobuf-based message encoding/decoding
- 🔄 Supports `SubscribeRequest` and `PublishRequest` messages
- 📚 Topic-based pub/sub architecture
- 🔁 Concurrent connection handling with a semaphore limit
- 💥 Graceful shutdown with `Ctrl+C` handling
- 🧪 Unit and integration tests included

---

## 📁 Project Structure

```text
.
├── main.rs             # Entry point of the application
├── lib.rs              # Core server implementation
├── appsettings.json    # Configuration file (required)
├── protocol/           # Protobuf-generated message definitions
└── topicmanager/       # Topic management logic
