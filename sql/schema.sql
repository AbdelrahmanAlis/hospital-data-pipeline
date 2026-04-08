drop table dim_doctors
drop table dim_patients
drop table dim_treatments
drop table fact_appointments
create table if not exists dim_doctors
(
	"doctor_id" int primary key,
    "full_name" text,
    "specialization" text,
    "years_experience" int,
    "hospital_branch" text
);

create table if not exists dim_patients
(
	"patient_id" int primary key,
    "full_name" text,
    "gender" text,
    "age" int,
    "address_region" text,
    "insurance_provider" text
);

create table if not exists dim_treatments
(
	"treatment_id" int primary key,
    "treatment_type" text,
    "description" text,
    "cost" real
);

CREATE TABLE IF NOT EXISTS dim_date (
    date_id INT PRIMARY KEY,
    full_date DATE NOT NULL,
    year INT NOT NULL,
    quarter INT NOT NULL,
    month_number INT NOT NULL,
    month_name VARCHAR(20) NOT NULL,
    day_of_week INT NOT NULL,
    day_name VARCHAR(20) NOT NULL,
    day_of_month INT NOT NULL,
    week_of_year INT NOT NULL,
    is_weekend BOOLEAN NOT NULL
);

CREATE OR REPLACE PROCEDURE sp_PopulateDateDimension(
    start_date DATE,
    end_date DATE
)
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO dim_date (
        date_id,
        full_date,
        year,
        quarter,
        month_number,
        month_name,
        day_of_week,
        day_name,
        day_of_month,
        week_of_year,
        is_weekend
    )
    SELECT 
        to_char(datum, 'YYYYMMDD')::INT,              -- date_sk
        datum,                                       -- full_date
        extract(year from datum),                    -- year
        extract(quarter from datum),                 -- quarter
        extract(month from datum),                   -- month_number
        to_char(datum, 'Month'),                     -- month_name
        extract(isodow from datum),                  -- day_of_week (1=Mon, 7=Sun)
        to_char(datum, 'Day'),                       -- day_name
        extract(day from datum),                     -- day_of_month
        extract(week from datum),                    -- week_of_year
        CASE 
            WHEN extract(isodow from datum) IN (6, 7) THEN TRUE 
            ELSE FALSE 
        END                                          -- is_weekend
    FROM generate_series(start_date, end_date, '1 day'::interval) datum
    ON CONFLICT (date_id) DO NOTHING;                -- Prevents errors if run twice
END;
$$;

CALL sp_PopulateDateDimension('2020-01-01', '2026-12-31');

create table if not exists fact_appointments
(
	"appointment_id" int primary key,
	"patient_id" int  references dim_patients(patient_id),
	"doctor_id" int  references dim_doctors(doctor_id),
	"treatment_id" int  references dim_treatments(treatment_id),
	"date_id" int,
	"appointment_timestamp" timestamp,
	"status" text,
	"amount" real,
	"reason_for_visit" text
);

DO $$
begin
	if not exists(
		select *
		from pg_constraint
		where conname = 'fk_doctor'
	)then
		alter table fact_appointments
		add constraint fk_doctor foreign key (doctor_id) references dim_doctors(doctor_id);
	end if;

	if not exists(
		select *
		from pg_constraint
		where conname = 'fk_patitent'
	)then
		alter table fact_appointments
		add constraint fk_patient foreign key (patient_id) references dim_patients(patient_id);
	end if;

	if not exists(
		select *
		from pg_constraint
		where conname = 'fk_treatment'
	)then
		alter table fact_appointments
		add constraint fk_treatment foreign key (treatment_id) references dim_treatments(treatment_id);
	end if;

	if not exists(
		select *
		from pg_constraint
		where conname = 'fk_date'
	)then
		alter table fact_appointments
		add constraint fk_date foreign key (date_id) references dim_date(date_id);
	end if;
end $$

select  * from fact_appointments
select * from dim_date
select * from dim_treatments
select * from dim_doctors
select * from dim_patients
