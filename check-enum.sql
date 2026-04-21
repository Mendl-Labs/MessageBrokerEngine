SELECT e.enumlabel FROM pg_enum e JOIN pg_type t ON e.enumtypid = t.oid WHERE t.typname = 'subscription_tier' ORDER BY e.enumsortorder;
SELECT column_name, data_type, udt_name FROM information_schema.columns WHERE table_name = 'tenants' AND column_name = 'subscription_tier';
