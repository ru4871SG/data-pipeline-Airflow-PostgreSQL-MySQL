-- Use this script to create the tables in the target db (production db) in PostgreSQL
-- This script will create the snowflake schema: with one fact table, dimension tables, and sub-dimension tables

BEGIN;

CREATE TABLE IF NOT EXISTS public.dim_customer
(
    customer_id integer NOT NULL,
    customer_name character varying(50) COLLATE pg_catalog."default",
    customer_email character varying(50) COLLATE pg_catalog."default",
    card_number character varying(15) COLLATE pg_catalog."default",
    customer_gender_id integer,
    CONSTRAINT customer_pkey PRIMARY KEY (customer_id)
);

CREATE TABLE IF NOT EXISTS public.dim_customer_gender
(
    customer_gender_id integer NOT NULL,
    gender_desc character varying(10) COLLATE pg_catalog."default",
    CONSTRAINT gender_pkey PRIMARY KEY (customer_gender_id)
);


CREATE TABLE IF NOT EXISTS public.dim_product
(
    product_id integer NOT NULL,
    product_name character varying(100) COLLATE pg_catalog."default",
    description character varying(250) COLLATE pg_catalog."default",
    product_price numeric(15, 2),
    product_type_id integer,
    CONSTRAINT product_pkey PRIMARY KEY (product_id)
);

CREATE TABLE IF NOT EXISTS public.dim_product_type
(
    product_type_id integer NOT NULL,
    product_type character varying(50) COLLATE pg_catalog."default",
    product_category character varying(50) COLLATE pg_catalog."default",
    CONSTRAINT product_type_pkey PRIMARY KEY (product_type_id)
);


CREATE TABLE IF NOT EXISTS public.dim_sales_outlet
(
    sales_outlet_id integer NOT NULL,
    sales_outlet_type character varying(20) COLLATE pg_catalog."default",
    outlet_address character varying(50) COLLATE pg_catalog."default",
    city_id integer,
    outlet_telephone character varying(15) COLLATE pg_catalog."default",
    outlet_postal_code integer,
    outlet_manager integer,
    CONSTRAINT sales_outlet_pkey PRIMARY KEY (sales_outlet_id)
);

CREATE TABLE IF NOT EXISTS public.dim_sales_outlet_city
(
    city_id integer NOT NULL,
    outlet_city character varying(40) COLLATE pg_catalog."default",
    CONSTRAINT sales_outlet_city_pkey PRIMARY KEY (city_id)
);


CREATE TABLE IF NOT EXISTS public.dim_staff
(
    staff_id integer NOT NULL,
    staff_first_name character varying(50) COLLATE pg_catalog."default",
    staff_last_name character varying(50) COLLATE pg_catalog."default",
    staff_position character varying(50) COLLATE pg_catalog."default",
    staff_location character varying(5) COLLATE pg_catalog."default",
    CONSTRAINT staff_pkey PRIMARY KEY (staff_id)
);


CREATE TABLE IF NOT EXISTS public.dim_date
(
    date_id integer NOT NULL,
    transaction_date date,
    day_of_week_id integer,
    month_id integer,
    year_id integer,
    CONSTRAINT dim_date_pkey PRIMARY KEY (date_id)
);

CREATE TABLE IF NOT EXISTS public.dim_day_of_week
(
    day_of_week_id integer NOT NULL,
    day_of_week character varying(10) COLLATE pg_catalog."default",
    CONSTRAINT day_of_week_pkey PRIMARY KEY (day_of_week_id)
);

CREATE TABLE IF NOT EXISTS public.dim_month
(
    month_id integer NOT NULL,
    month_name character varying(10) COLLATE pg_catalog."default",
    CONSTRAINT month_pkey PRIMARY KEY (month_id)
);

CREATE TABLE IF NOT EXISTS public.dim_year
(
    year_id integer NOT NULL,
    year integer,
    CONSTRAINT year_pkey PRIMARY KEY (year_id)
);

CREATE TABLE IF NOT EXISTS public.fact_sales
(
    transaction_id integer NOT NULL,
    date_id integer,
    sales_outlet_id integer,
    staff_id integer,
    product_id integer,
    customer_id integer,
    quantity integer,
    price numeric(15, 2),
    CONSTRAINT sales_transaction_pkey PRIMARY KEY (transaction_id)
);


ALTER TABLE IF EXISTS public.dim_product
    ADD CONSTRAINT product_product_type_id_fkey FOREIGN KEY (product_type_id)
    REFERENCES public.dim_product_type (product_type_id) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION;

ALTER TABLE IF EXISTS public.dim_customer
    ADD CONSTRAINT customer_customer_gender_id_fkey FOREIGN KEY (customer_gender_id)
    REFERENCES public.dim_customer_gender (customer_gender_id) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION;

ALTER TABLE IF EXISTS public.dim_sales_outlet
    ADD CONSTRAINT sales_outlet_city_id_fkey FOREIGN KEY (city_id)
    REFERENCES public.dim_sales_outlet_city (city_id) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION;

ALTER TABLE IF EXISTS public.dim_date
    ADD CONSTRAINT dim_date_day_of_week_id_fkey FOREIGN KEY (day_of_week_id)
    REFERENCES public.dim_day_of_week (day_of_week_id) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION;

ALTER TABLE IF EXISTS public.dim_date
    ADD CONSTRAINT dim_date_month_id_fkey FOREIGN KEY (month_id)
    REFERENCES public.dim_month (month_id) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION;

ALTER TABLE IF EXISTS public.dim_date
    ADD CONSTRAINT dim_date_year_id_fkey FOREIGN KEY (year_id)
    REFERENCES public.dim_year (year_id) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION;


ALTER TABLE IF EXISTS public.fact_sales
    ADD CONSTRAINT sales_transaction_customer_id_fkey FOREIGN KEY (customer_id)
    REFERENCES public.dim_customer (customer_id) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION;

ALTER TABLE IF EXISTS public.fact_sales
    ADD CONSTRAINT sales_transaction_date_id_fkey FOREIGN KEY (date_id)
    REFERENCES public.dim_date (date_id) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION;

ALTER TABLE IF EXISTS public.fact_sales
    ADD CONSTRAINT sales_transaction_sales_outlet_id_fkey FOREIGN KEY (sales_outlet_id)
    REFERENCES public.dim_sales_outlet (sales_outlet_id) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION;

ALTER TABLE IF EXISTS public.fact_sales
    ADD CONSTRAINT sales_transaction_staff_id_fkey FOREIGN KEY (staff_id)
    REFERENCES public.dim_staff (staff_id) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION;

ALTER TABLE IF EXISTS public.fact_sales
    ADD CONSTRAINT sales_transaction_product_id_fkey FOREIGN KEY (product_id)
    REFERENCES public.dim_product (product_id) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION;

END;