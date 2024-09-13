DO
$$
BEGIN
   -- Check if the role 'admin' exists
   IF NOT EXISTS (
      SELECT FROM pg_catalog.pg_roles WHERE rolname = 'admin'
   ) THEN
      -- Create the 'admin' role if it does not exist
      CREATE ROLE admin WITH LOGIN PASSWORD 'admin_password';
      -- Grant necessary privileges (optional)
      GRANT ALL PRIVILEGES ON DATABASE airflow_db TO admin;
   END IF;
END
$$;
