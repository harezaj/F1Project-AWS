CREATE TABLE drivers.driver_info
(
    driver_id integer NOT NULL GENERATED ALWAYS AS IDENTITY,
    ref_name text, 
    first_name text,
    last_name text,
    date_of_birth date,
    nationality text,
    permanent_number integer,
    code text,
    CONSTRAINT drivers_pkey PRIMARY KEY (driver_id),
    CONSTRAINT driver_ref_name_unique UNIQUE (ref_name)
)