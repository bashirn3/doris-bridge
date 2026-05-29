-- Reversible CSV fallback for TJ lead sourcing.
--
-- Apply this in Supabase SQL editor before importing the DORIS customer CSV.
-- Rollback path:
--   update tj_config set lead_source = 'doris', updated_at = now() where id = 'main';
--   -- optional archive/delete:
--   -- delete from tj_csv_leads where source_batch_id = 'csv_dump_2026_05_28';

create table if not exists public.tj_csv_leads (
  id bigserial primary key,
  source_batch_id text not null,
  source_key text not null,
  status text not null default 'pending'
    check (status in ('pending', 'contacted', 'booked', 'skipped', 'archived')),
  lead_type text not null
    check (lead_type in ('due_soon', 'passed', 'not_due', 'unknown')),
  first_name text,
  last_name text,
  name text,
  phone text,
  normalized_phone text,
  email text,
  doris_customer_id text,
  customer_status text,
  registration text,
  vehicles text,
  vehicle_class text,
  station_id integer,
  station_name text,
  last_inspection_date date,
  next_inspection_date date,
  days_until_deadline integer,
  months_since_inspection numeric,
  reminder_email_at date,
  reminder_sms_at date,
  marketing_email_at date,
  marketing_sms_at date,
  invite_sent_at date,
  invite_channel text,
  invited_registration text,
  skip_reason text,
  raw_row jsonb not null default '{}'::jsonb,
  imported_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  contacted_at timestamptz,
  booked_at timestamptz,
  unique (source_batch_id, source_key)
);

create index if not exists tj_csv_leads_status_type_idx
  on public.tj_csv_leads (status, lead_type, next_inspection_date);

create index if not exists tj_csv_leads_phone_idx
  on public.tj_csv_leads (normalized_phone);

create index if not exists tj_csv_leads_registration_idx
  on public.tj_csv_leads (registration);

create index if not exists tj_csv_leads_batch_idx
  on public.tj_csv_leads (source_batch_id);

create table if not exists public.tj_config (
  id text primary key,
  updated_at timestamptz not null default now()
);

alter table public.tj_config
  add column if not exists lead_source text not null default 'doris'
  check (lead_source in ('doris', 'csv'));

insert into public.tj_config (id, lead_source, updated_at)
values ('main', 'doris', now())
on conflict (id) do nothing;
