-- medical_providers table for Lakebase database databricks_postgres, schema public.
-- Source export: medical_providers.parquet (from SLED Workshop Lakebase dbo.medical_providers).

CREATE TABLE IF NOT EXISTS public.medical_providers (
    medicaid_provider_id INTEGER,
    npi BIGINT,
    provider_or_facility_name TEXT,
    medicaid_type TEXT,
    profession_or_service TEXT,
    provider_specialty TEXT,
    service_address TEXT,
    city TEXT,
    state TEXT,
    zip_code TEXT,
    county TEXT,
    telephone BIGINT,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    enrollment_begin_date DATE,
    next_anticipated_revalidation_date DATE,
    file_date DATE,
    medically_fragile_children_and_adults_directory_ind TEXT,
    provider_email TEXT
);
