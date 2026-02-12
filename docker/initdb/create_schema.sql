CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS rejects;
CREATE SCHEMA IF NOT EXISTS  staging;
CREATE SCHEMA IF NOT EXISTS  intermediate;
CREATE SCHEMA IF NOT EXISTS  marts;

  CREATE TABLE IF NOT EXISTS raw.google_ads_raw (
      row_num text,
      campaign_id text,
      campaign_name text,
      campaign_type text,
      status text,
      date text,
      impressions text,
      clicks text,
      cost_micros text,
      conversions text,
      conversion_value text,
      run_id text,
      load_ts text,
      load_date text
  );

  CREATE TABLE IF NOT EXISTS raw.facebook_export_raw (
      row_num text,
      campaign_id text,
      campaign_name text,
      date text,
      impressions text,
      clicks text,
      spend text,
      purchases text,
      purchase_value text,
      reach text,
      frequency text,
      run_id text,
      load_ts text,
      load_date text
  );

  CREATE TABLE IF NOT EXISTS raw.crm_revenue_raw (
      row_num text,
      order_id text,
      customer_id text,
      order_date text,
      revenue text,
      channel_attributed text,
      campaign_source text,
      product_category text,
      region text,
      run_id text,
      load_ts text,
      load_date text
  );