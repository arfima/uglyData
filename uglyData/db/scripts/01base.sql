SET SESSION AUTHORIZATION dbadm;
CREATE SCHEMA IF NOT EXISTS info;

CREATE TABLE IF NOT EXISTS info.exchanges (
    mic text PRIMARY KEY,
    description text,
    url text,
    arfimaname text,
    comment text,
    tt_ticker text
);

CREATE TABLE IF NOT EXISTS info.families (
    family text PRIMARY KEY,
    description text
);

CREATE TABLE IF NOT EXISTS info.subfamilies (
    subfamily text PRIMARY KEY,
    family text REFERENCES info.families ON DELETE RESTRICT ON UPDATE CASCADE,
    description text
);

CREATE TABLE IF NOT EXISTS info.curves (
    curve text PRIMARY KEY,
    description text
);

CREATE TABLE IF NOT EXISTS info.curves_bd (
    curve_bd text PRIMARY KEY,
    description text
);
DO $$ BEGIN
    CREATE TYPE info.yield_types AS ENUM ('none', 'dlv', 'direct','hundred_minus');
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;

CREATE TABLE IF NOT EXISTS info.products (
    product text,
    exchange text REFERENCES info.exchanges (mic) ON DELETE RESTRICT ON UPDATE CASCADE,
    product_type text,
    description text,
    family text REFERENCES info.families ON DELETE RESTRICT ON UPDATE CASCADE,
    subfamily text REFERENCES info.subfamilies ON DELETE RESTRICT ON UPDATE CASCADE,
    curve text REFERENCES info.curves ON DELETE RESTRICT ON UPDATE CASCADE,
    curve_bd text REFERENCES info.curves_bd ON DELETE RESTRICT ON UPDATE CASCADE,
    listed_contracts smallint,
    listed_contracts_letters text,
    listed_contracts_liquid smallint,
    listed_contracts_offcycle smallint,
    listed_contracts_offcycle_letters text,
    nominal decimal,
    dv01 decimal,
    pv01 decimal,
    currency text,
    bloomberg_ticker text,
    bloomberg_suffix text,
    refinitiv_ticker text,
    tt_ticker text,
    exchange_ticker text,
    eod_columns text[],
    intraday_tables text[],
    db2tt_factor decimal,
    ticksize decimal,
    last_tradable_time text,
    holidays text,
    has_delivery boolean,
    url text,
    intra_source text,
    eod_source text,
    t2t boolean,
    eod_prod_columns text[],
    spread_distance jsonb,
    column_fields_override jsonb,
    seasonal_factor_close decimal,
    seasonal_factor_early_close decimal,
    seasonal_reference_instrument text,
    wb_ticker text,
    cmt_tenor decimal,
    cmt_spline_tenor decimal,
    yield_type info.yield_types default 'none',
    PRIMARY KEY (product, product_type)
);

CREATE TABLE IF NOT EXISTS info.instruments(
    instrument text PRIMARY KEY,
    product text,
    product_type text,
    first_tradeable_date date,
    last_tradeable_date timestamptz,
    first_delivery_date date,
    last_delivery_date date,
    underlying_coupon numeric,
    description text,
    refinitiv_ticker text,
    bloomberg_ticker text,
    bloomberg_suffix text,
    column_fields_override jsonb,
    seasonal_factor_close numeric,
    seasonal_factor_early_close numeric,
    FOREIGN KEY (product, product_type) REFERENCES info.products ON DELETE RESTRICT ON UPDATE CASCADE
);

CREATE TABLE IF NOT EXISTS info.columns(
    column_name text PRIMARY KEY,
    description text,
    field_tt text,
    field_bloomberg text,
    field_refinitiv text,
    field_rjo text,
    field_wb text,
    matlab_letter text,
    field_eikon_ndaraw text,
    field_eikon_da text,
    field_eikon_da_name text
);


CREATE TABLE IF NOT EXISTS info.tsyyieldconstant(
    instrument text REFERENCES info.instruments ON DELETE RESTRICT ON UPDATE CASCADE PRIMARY KEY,
    roll_constant decimal
);

CREATE TABLE IF NOT EXISTS info.deliverables(
    instrument text REFERENCES info.instruments ON DELETE RESTRICT ON UPDATE CASCADE,
    deliverable_isin text,
    conversion_factor decimal,
    has_been_cheapest boolean,
    PRIMARY KEY (instrument, deliverable_isin)
);

CREATE TABLE IF NOT EXISTS info.bonds(
    isin text PRIMARY KEY,
    bond_name text,
    cusip text,
    coupon decimal,
    maturity DATE,
    left_wing text REFERENCES info.bonds,
    right_wing text REFERENCES info.bonds,
    issue_date DATE,
    issuer text,
    coupon_type text,
    coupon_formula text,
    day_count_convention text,
    ammount_issued decimal,
    ammount_outstanding decimal,
    first_accrual_date DATE,
    first_coupon_date DATE,
    country_of_risk text,
    currency text,
    coupon_freq numeric
);

CREATE TABLE IF NOT EXISTS info.ecoreleases(
    instrument text PRIMARY KEY,
    product text, 
    product_type text,
    download_polls text, 
    description text,
    bloomberg_ticker text,
    bloomberg_suffix text,
    old_name text,
    country text,
    last_release_ticker text,
    polls_median_ticker text,
    polls_mean_ticker text,
    polls_low_ticker text,
    polls_high_ticker text,
    first_release_ticker text,
    frequency text,
    eod_source text
);

CREATE TABLE IF NOT EXISTS info.data_sources (
    source_name text PRIMARY KEY,
    description text,
    eikon_view text
);

CREATE TABLE IF NOT EXISTS info.timescale_config (
    tabtype text PRIMARY KEY,
    chunk_interval INTERVAL,
    compress_orderby text,
    compress_segmentby text,
    compression_policy INTERVAL
);

DO $$ BEGIN
    CREATE TYPE info.data_type AS ENUM ('intraquote', 'intrade', 'trades', 'quotes', 'eod', 'opteod');
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;

CREATE TABLE IF NOT EXISTS info.custom_indices (
    custom_index text primary KEY,
    description text,
    class_name text
);

CREATE TABLE IF NOT EXISTS info.drivers (
	driver text primary KEY,
	description text,
	tags text[], 
	dtype info.data_type,
	store boolean
);

CREATE TABLE IF NOT EXISTS info.drivers_legs (
	driver text,
	weight numeric,
	instrument text,
	attr text,
	roll_method text,
	FOREIGN KEY (driver) REFERENCES info.drivers(driver) ON UPDATE CASCADE
);



CREATE TABLE IF NOT EXISTS info.spreads (
	arfima_name text primary KEY,
	violet_name text, 
	name_short text,
	auto_scalper_name text,
	scalper_mode text,
	violet_display_name text,
	violet_portfolio_name text,
	master_portfolio_name text,
	constant numeric,
	constant_yield numeric,
	constant_yield_th numeric,
	tick_size numeric,
	tt_formula text, 
	currency text,
	market_scale_factor numeric,
	fees numeric,
	dv01 numeric,
	pv01 numeric,
	rules text[],
	rules_sheet text,
	using_user_defined_ticksize boolean,
	user_defined_numerator numeric,
	user_defined_denominator numeric,
	violet_ticket_name text,
	qm_parameters JSONB,
	rs_parameters JSONB
);


CREATE TABLE IF NOT EXISTS info.spreads_executions(
	execution_id SERIAL primary key, 
	spread text references info.spreads(arfima_name) on update cascade on delete cascade, 
	quoters text[],
	scenarios text[],
	ticket_parameters JSONB
);


CREATE TABLE IF NOT EXISTS info.spreads_legs(
	instrument text references info.instruments on update cascade,
	execution_id int references info.spreads_executions(execution_id) on update cascade on delete cascade,
	weight_spread numeric,
	weight_price numeric,
	weight_yield numeric,
	weight_yield_th numeric,
	weight_yield_cmt numeric,
	is_lean_indicative boolean,
	is_hedging boolean,
	min_lean_qty text,
	payup_ticks numeric,
	queue_holder_orders int,
	hedge_qty int,
	lean_qty int,
	active_quoting boolean,
	tt_account text,
	quote_max_aggr numeric,
	primary key (execution_id, instrument)
);

CREATE TABLE IF NOT EXISTS info.events (
    id SERIAL primary key,
    start_date date NOT NULL,
    start_dt timestamptz,
    end_date date NOT NULL,
    end_dt timestamptz,
    event_name text,
    event_category text NOT NULL,
    event_subcategory text,
    description text,
    event_analysis text,
    other_information jsonb,
    event_short_name text,
    event_origin text,
    peak_date date,
    peak_dt timestamptz,
    credit_tag text
);


DO $$ BEGIN
    CREATE TYPE action_type as enum ('CREATE', 'UPDATE', 'DELETE');
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;

CREATE TABLE IF NOT EXISTS info.frontend_log (
    id SERIAL primary key,
    dtime TIMESTAMP with TIME ZONE,
    action_type action_type,
    user_name text,
    schema_name text,
    table_name text,
    old_data jsonb,
    new_data jsonb
);



CREATE OR REPLACE VIEW info.drivers_view  as (
select d.*, legs.legs from info.drivers d
full outer  join (
	select driver, jsonb_agg(json_build_object('weight', weight, 'instrument', instrument, 'attr', attr, 'roll_method', roll_method)) as legs 
	from info.drivers_legs 
	group by driver
) legs on d.driver = legs.driver
);


CREATE TABLE IF NOT EXISTS info.ecoreleases
(
    instrument text PRIMARY KEY,
    product text, 
    product_type text,  
    download_polls text,  
    description text,  
    bloomberg_ticker text,  
    bloomberg_suffix text,  
    old_name text,  
    country text,  
    last_release_ticker text,  
    polls_median_ticker text,  
    polls_mean_ticker text,  
    polls_low_ticker text,  
    polls_high_ticker text,  
    first_release_ticker text,  
    frequency text,  
    eod_source text 
);


CREATE TABLE IF NOT EXISTS info.tags
(
    tag text
);

CREATE TABLE IF NOT EXISTS info.tag_products
(
    tag text,
    product text,
    product_type text
);

CREATE TABLE IF NOT EXISTS info.tag_instruments
(
    tag text,
    instrument text
);

CREATE TABLE IF NOT EXISTS info.tag_strategy_filters
(
    tag text,
    strategy_filter text
);

CREATE TABLE IF NOT EXISTS info.tag_custom_filters (
    tag text,
    custom_instrument_filter text,
    PRIMARY KEY (tag, custom_instrument_filter)
);

CREATE OR REPLACE VIEW info.product_tags
AS
SELECT p.*, tp.tags FROM info.products p
LEFT JOIN (SELECT array_agg(itp.tag) AS tags,
    itp.product, itp.product_type
    FROM info.tag_products itp
GROUP BY itp.product, itp.product_type) tp ON tp.product = p.product AND tp.product_type = p.product_type;

CREATE TABLE IF NOT EXISTS info.marketdata_tags --in the db is a view
(
    instrument text,
    tags text[]
);


CREATE OR REPLACE VIEW info.spreads_view as
SELECT sv.*, mkt.tags FROM (SELECT s.*, json_agg(json_build_object(
		'execution_id',e.execution_id,
		'quoters', e.quoters,
		'scenarios', e.scenarios,
		'ticket_parameters', ticket_parameters,
		'legs', l.legs
		)) AS executions
FROM info.spreads s
LEFT JOIN info.spreads_executions e ON s.arfima_name = e.spread
LEFT JOIN (
    SELECT execution_id, json_agg(l.*) AS legs
    FROM info.spreads_legs l
    GROUP BY execution_id
) l ON e.execution_id = l.execution_id
GROUP BY s.arfima_name) sv
LEFT JOIN info.marketdata_tags mkt ON (regexp_replace(sv.arfima_name, '(.{3}$)', 'S\1')
  || 'EX') = mkt.instrument;
------ inserts

create table info.instruments_etal (
    instrument text primary key,
    dtype text 
);

insert into info.instruments_etal values
    ('LOIS3M', 'eod'),
    ('TESTDRIVER', 'eod'),
    ('EDG1NR', 'eod'),
    ('EDG2NR', 'eod'),
    ('USSOC11H', 'eod'),
    ('LIBOR3MIDX', 'eod');




INSERT INTO info.exchanges VALUES
    ('CBOT', 'Chicago Board of Trade', 'CME'),
    ('CME', 'Chicago Mercantile Exchange', 'CME'),
    ('NYMEX', 'New York Mercantile Exchange', 'CME'),
    ('CFE', 'Cboe Futures Exchange', 'CFE'),
    ('ASX', 'Australian Securities Exchange', 'ASX'),
    ('MX', 'Montréal Exchange', 'MX'),
    ('Eurex', 'Eurex Exchange', 'Eurex'),
    ('ICE', 'Intercontinental Exchange', 'ICE'),
    ('ICEL', 'ICE Futures Europe (prev. LIFFE)', 'ICEL');


INSERT INTO info.families VALUES
    ('Fixed Income', 'Fixed Income. Whatever you want to tell about this family.'),
    ('Indices', 'Indices and indices'' futures.'),
    ('Equity', 'Equity futures.'),
    ('Commodities', 'Commodity futures.'),
    ('Cryptos', 'Cryptocurrencies'' indices and futures.'),
    ('Currencies', 'Currency futures.');


INSERT INTO info.subfamilies VALUES
    ('STIRS', 'Fixed Income', 'Short Term Interest Rates.'),
    ('USTsy', 'Fixed Income', 'US Treasury Futures.'),
    ('CADTsy', 'Fixed Income', 'Canadian Treasury Futures.'),
    ('Equity', 'Indices', 'Equity indices and futures.'),
    ('Volatility', 'Indices', 'Volatility futures.'),
    ('Dividends', 'Commodities', 'Dividend futures.'),
    ('Energy', 'Commodities', 'Energy futures.'),

    ('Index', 'Indices', 'Index Futures.'),
    -- PLaceholders
    ('Commodities subfamily default', 'Commodities', 'Placeholder'),

    ('Equity subfamily default', 'Equity', 'Placeholder'),
    ('FI subfamily default', 'Fixed Income', 'Placeholder');


INSERT INTO info.curves VALUES
    ('ED', 'Eurodollars curve.'),
    ('FF', 'FedFunds curve.'),
    ('USTsy', 'US Treasury curve.'),
    ('Volatility', 'Volatility curve.'),
    ('Natural Gas', 'Natural gas curve.');


INSERT INTO info.curves_bd VALUES
    ('ED', 'Eurodollars curve.'),
    ('FF', 'FedFunds curve.'),
    ('USTsy', 'US Treasury curve.'),
    ('FVS', 'FVS curve.'),
    ('UX', 'UX curve.'),
    ('Vanilla', 'Vanilla curve.');


INSERT INTO info.products VALUES
--     ('product', 'exchange', 'product_type', 'description', 'family', 'subfamily', 'curve', 'curve_bd', 'listed_contracts', 'nominal', 'dv01', 'currency', 'bloomberg_ticker', 'bloomberg_suffix', 'refinitiv_ticker', 'tt_ticker', 'exchange_ticker', 'eod_columns', 'db2tt_factor', 'ticksize', last_tradable_time)
--     ('product', 'exchange', 'product_type', 'description', 'family', 'subfamily', 'curve', 'curve_bd', 'listed_contracts', 'nominal', 'dv01', 'currency', 'bloomberg_ticker', 'bloomberg_suffix', 'refinitiv_ticker', 'tt_ticker', 'exchange_ticker', 'eod_columns', 'intraday_columns', 'db2tt_factor', 'ticksize', 'last_tradable_time', 'holidays', 'has_delivery')
--     ('product', 'exchange', 'product_type', 'description', 'family', 'subfamily', 'curve', 'curve_bd', 'listed_contracts', 'listed_contracts_letters', 'listed_contracts_liquid', 'listed_contracts_offcycle', 'listed_contracts_offcycle_letters', 'nominal', 'dv01', 'pv01', 'currency', 'bloomberg_ticker', 'bloomberg_suffix', 'refinitiv_ticker', 'tt_ticker', 'exchange_ticker', 'eod_columns', 'intraday_columns', 'db2tt_factor', 'ticksize', 'last_tradable_time', 'holidays', 'has_delivery', url, intra_source, eod_source, t2t, eod_prod_columns)
--     ('product', 'exchange', 'product_type', 'description', 'family', 'subfamily', 'curve', 'curve_bd', 'listed_contracts', 'listed_contracts_letters', 'listed_contracts_liquid', 'listed_contracts_offcycle', 'listed_contracts_offcycle_letters', 'nominal', 'dv01', 'pv01', 'currency', 'bloomberg_ticker', 'bloomberg_suffix', 'refinitiv_ticker', 'tt_ticker', 'exchange_ticker', 'eod_columns', 'intraday_columns', 'db2tt_factor', 'ticksize', 'last_tradable_time', 'holidays', 'has_delivery', url, intra_source, eod_source, t2t, eod_prod_columns, spread_distance ,column_fields_override jsonb, seasonal_factor_close , seasonal_factor_early_close , seasonal_reference_instrument , wb_ticker) NULL,NULL,NULL,NULL,NULL,NULL,
    ('ED', 'CME', 'Outright', 'Eurodollar Future Outrights', 'Fixed Income', 'STIRS', 'ED', 'ED','40','HMUZ','40','4','FGJKNQVX', '250000', '25',NULL, 'USD', 'ED', 'Comdty', 'ED', 'GE', 'GE', '{"dtime", "open", "high", "low", "close", "settle", "volume", "open_interest"}', '{}','100', '0.005', '11:00 Europe/London', 'Financial_Markets_UK','false', NULL, NULL, NULL, NULL, NULL, NULL),
    ('FF','CME','Calendar','Fed funds Calendars','Fixed Income','STIRS','FF', 'FF', '59','FGHJKMNQUVXZ','10',NULL,'', '416700','41.67',NULL,'USD','FF','Comdty','FF','ZQ','ZQ','{"dtime", "open", "high", "low", "close", "settle", "volume", "open_interest"}','{}','1','0.005', '23:59 America/Chicago', 'CBOT_InterestRate','false', NULL, NULL, NULL, NULL, NULL, NULL),
    ('TU','CBOT','Outright','2 year Treasury Note Futures','Fixed Income','USTsy','USTsy','USTsy','3','HMUZ','40','4','FGJKNQVX','200000','25.0',NULL,'USD','TU','Comdty','TU','ZT','ZT','{"dtime", "open", "high", "low", "close", "settle", "volume", "open_interest"}','{}','1','0.005', '12:01 US/Central', 'CBOT_InterestRate','true', NULL, NULL, NULL, NULL, NULL, NULL),
    ('FVS','Eurex','Outright','VSTOXX Volatility Index Futures','Indices','Volatility','Volatility','FVS','8','HMUZ','40','4','FGJKNQVX','100','100',NULL,'EUR','FVS','Index','FVS','FVS','FVS','{"dtime", "open", "high", "low", "close", "settle", "volume", "open_interest"}','{}','1','0.05', '12:00 CET', 'EUREX','false', NULL, NULL, NULL, NULL, NULL, NULL),
    ('UX','CFE','Outright','VIX Volatility Index Futures','Indices','Volatility','Volatility','UX','8','HMUZ','40','4','FGJKNQVX','1000','1000',NULL,'USD','UX','Index','VX','VX','VX','{"dtime", "open", "high", "low", "close", "settle", "volume", "open_interest"}','{}','1','0.05', '15:00 US/Central', 'CFE','false', NULL, NULL, NULL, NULL, NULL, NULL),
    ('NG','NYMEX','Outright','Natural gas Futures','Commodities','Energy','Natural Gas','Vanilla','27','HMUZ','40','4','FGJKNQVX','10000','10000.0',NULL,'USD','NG','Comdty','NG','NG','NG','{"dtime", "open", "high", "low", "close", "settle", "volume", "open_interest"}','{}','1','0.001', '23:59 US/Central', 'CMEGlobex_NatGas','false', NULL, NULL, NULL, NULL, NULL, NULL),
    ('IW','NYMEX','Outright','HenryHub Natural Gas Lastday Financial Futures','Commodities','Energy','Natural Gas','Vanilla','15','HMUZ','40','4','FGJKNQVX','10000','10000.0',NULL,'USD','IW','Comdty','','HH','HH','{"dtime", "open", "high", "low", "close", "settle", "volume", "open_interest"}','{}','1','0.001', '23:59 US/Central', 'CMEGlobex_NatGas','false', NULL, NULL, NULL, NULL, NULL, NULL),
    ('SX5E', 'CME', 'Option', 'HenryHub Natural Gas Lastday Financial Futures','Commodities','Energy','Natural Gas','Vanilla','15','HMUZ','40','4','FGJKNQVX','10000','10000.0',NULL,'USD','IW','Comdty','','HH','HH','{"dtime", "open", "high", "low", "close", "settle", "volume", "open_interest"}','{}','1','0.001', '23:59 US/Central', 'CMEGlobex_NatGas','false', NULL, NULL, NULL, NULL, NULL, NULL);

INSERT INTO info.instruments VALUES
--     ('instrument', 'product', 'exchange', 'product_type', 'first_tradeable_date', 'last_tradeable_date', 'first_delivery_date', 'last_delivery_date');
    ('EDH23', 'ED', 'Outright', '20130319T00:00:00Z', '2023-03-13 11:00:00 Europe/London', NULL, NULL),
    ('EDM22', 'ED', 'Outright', '20130319T00:00:00Z', '2023-03-13 11:00:00 Europe/London', NULL, NULL),
    ('FFZ22F23', 'FF', 'Calendar', '20200102T00:00', '2022-12-30 23:59 America/Chicago', NULL, NULL),
    ('TUH23', 'TU', 'Outright', '20220107', '20230331', '2023-03-01 12:01 US/Central', '20230405'),
    ('FVSZ22', 'FVS','Outright', '20220420', '2022-12-21 12:00 CET', NULL, NULL),
    ('SX5EIDXC20230915M2850', 'SX5E', 'Option', NULL, NULL, NULL, NULL),
    ('SX5EIDXC20230915M3000', 'SX5E', 'Option', NULL, NULL, NULL, NULL),
    ('SX5EIDXC20230915M5000', 'SX5E', 'Option', NULL, NULL, NULL, NULL),
    ('UXZ22', 'UX','Outright', '20220320', '2022-12-21 15:00 US/Central', NULL, NULL);

INSERT INTO info.columns values
    ('test_column', NULL, NULL, NULL, NULL, NULL, NULL);


INSERT INTO info.drivers(driver, description, tags, dtype, store) VALUES
    ('LOIS3M','LOIS 3M','{}','eod',False),
    ('USSOC11H','USSOT3MDX at 11h','{}','eod',False),
    ('TESTDRIVER','Test driver','{}','eod',False);

INSERT INTO info.drivers_legs VALUES
    ('LOIS3M',	-100, 	'LIBOR3MIDX'	,'settle', NULL),
    ('LOIS3M',	100	, 'USSOC11H'	,'trade_price', NULL),
    ('TESTDRIVER', 100, 'EDG1NR', 'close', NULL),
    ('TESTDRIVER', 1, 'EDG2NR', 'close', NULL);


INSERT INTO info.events (id, start_date, start_dt, end_date, end_dt, event_name, event_category, event_subcategory, description, event_analysis, other_information, event_short_name, event_origin, peak_date, peak_dt, credit_tag) VALUES 
 (DEFAULT, '2021-01-01', NULL, '2021-01-01', NULL, 'ECB meeting', 'CategoryA', NULL, 'Example event', NULL, NULL, NULL, NULL, NULL, NULL, NULL),
 (DEFAULT, '2021-01-01', NULL, '2021-01-01', NULL, 'ECB meeting', 'CategoryB', NULL, 'Example event', NULL, NULL, NULL, NULL, NULL, NULL, NULL);


INSERT INTO info.bonds (isin, bond_name, cusip, coupon, maturity, left_wing, right_wing, issue_date, issuer, coupon_type, coupon_formula, day_count_convention, ammount_issued, ammount_outstanding, first_accrual_date, first_coupon_date, country_of_risk, currency, coupon_freq) 
VALUES 
    ('US912828YY08','T 1.75 12/31/2024','912828YY0',1.75,'12/31/2024','US912828YV68','US912828Z526','12/31/2019','United States of America (Government)','Plain Vanilla Fixed Coupon',NULL,'Actual/Actual',44858810600,44858744400,'12/31/2019','6/30/2020','USA','USD',2),
    ('US91282CGD74','T 4.25 12/31/2024','91282CGD7',4.25,'12/31/2024','US912828YV68','US912828Z526','1/3/2023','United States of America (Government)','Plain Vanilla Fixed Coupon',NULL,'Actual/Actual',42000043000,41988991200,'12/31/2022','6/30/2023','USA','USD',2),
    ('US912828YV68','T 4.25 12/31/2024','91282CGD7', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
    ('US912828Z526','T 4.25 12/31/2024','91282CGD7', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL); 


INSERT INTO info.deliverables(
	instrument, deliverable_isin, conversion_factor, has_been_cheapest)
	VALUES 
    ('TUH23','US912828YY08',0.9303,True),
    ('TUH23','US912828Z526',0.9208,False);



INSERT INTO info.ecoreleases(
	instrument, product, product_type, download_polls, description, bloomberg_ticker, bloomberg_suffix, old_name, country, last_release_ticker, polls_median_ticker, polls_mean_ticker, polls_low_ticker, polls_high_ticker, first_release_ticker, frequency, eod_source)
	VALUES 
('XTSBEZECI', 'XTSBEZ', 'ECI', 'FALSE', 'TB Eurozone', 'XTSBEZ', 'Index', 'XTSBEZIndex', 'EEE', 'EUTBSA=ECI', 'pEUTBSA=M', 'pEUTBSA=E', 'pEUTBSA=L', 'pEUTBSA=H', NULL, 'monthly', 'LSEG'),
( 'WMCCCONECI', 'WMCCCON', 'ECI', 'FALSE', 'CC (Westpac) AUS', 'WMCCCON%', 'Index', 'WMCCCONIndex', 'AUS', 'AUCONS=ECI', 'pAUCONS=M', 'pAUCONS=E', 'pAUCONS=L', 'pAUCONS=H', NULL, 'monthly', 'LSEG');


create schema primarydata;

create table primarydata.instruments_info (
    instrument text,
    dtype text,
    "start" timestamp with time zone,
    "end" timestamp with time zone
);

insert into primarydata.instruments_info values
    ('EDH23', 'eod', '2010-01-01', '2023-04-01'),
    ('EDH23', 'trades', '2010-01-01', '2023-04-01'),
    ('EDH23', 'quotes', '2010-01-01', '2023-04-01'),
    ('FFZ22F23', 'trades', NULL, NULL),
    ('TUH23', 'quotes', NULL, NULL),
    ('FVSZ22', 'intraquote', NULL, NULL),
    ('UXZ22', 'intrade', NULL, NULL),
    ('SX5EIDXC20230915M2850', 'opteod', '2010-01-01', '2023-04-01');

CREATE TABLE IF NOT EXISTS primarydata.base_cheapest
(
    dtime date NOT NULL,
    instrument text  NOT NULL,
    cheapest text ,
    cheapest_fixed text
);



--auth schema
CREATE SCHEMA IF NOT EXISTS auth;

DO $$ BEGIN
    CREATE TYPE auth.user_access AS ENUM ('none', 'read', 'write', 'admin');
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;

CREATE TABLE IF NOT EXISTS auth.users (
    username text PRIMARY KEY,
    name text,
    products auth.user_access default 'read',
    instruments auth.user_access default 'read',
    drivers auth.user_access default 'read',
    users auth.user_access default 'none',
    exchanges auth.user_access default 'read',
    families auth.user_access default 'read',
    subfamilies auth.user_access default 'read',
    events auth.user_access default 'read',
    frontend_log auth.user_access default 'read',
    columns auth.user_access default 'read',
    spreads auth.user_access default 'read',
    market auth.user_access default 'read',
    tags auth.user_access default 'read'
);

CREATE EXTENSION unaccent;

CREATE OR REPLACE VIEW auth.users_unaccent AS
    SELECT *, unaccent(name) AS name_unaccented FROM auth.users;

insert into auth.users values
    ('test1', 'Test User 1', 'admin', 'admin', 'admin', 'admin', 'admin', 'admin', 'admin', 'admin', 'admin', 'admin', 'admin', 'admin', 'admin'),
    ('test2', 'Test User 2', 'read', 'read', 'read', 'none', 'read', 'read', 'read', 'read', 'read', 'read', 'read', 'read', 'read');


CREATE OR REPLACE FUNCTION GapFillInternal( 
    s anyelement, 
    v anyelement) RETURNS anyelement AS 
$$ 
BEGIN 
  RETURN COALESCE(v,s); 
END; 
$$ LANGUAGE PLPGSQL IMMUTABLE; 

CREATE AGGREGATE fill_na(anyelement) ( 
  SFUNC=GapFillInternal, 
  STYPE=anyelement 
); 


INSERT INTO info.spreads (arfima_name) VALUES ('spread1');
INSERT INTO info.spreads (arfima_name) VALUES ('spread2');


INSERT INTO info.spreads_executions (execution_id, spread) VALUES (DEFAULT, 'spread1');
INSERT INTO info.spreads_executions (execution_id, spread) VALUES (DEFAULT, 'spread1');
INSERT INTO info.spreads_executions (execution_id, spread) VALUES (DEFAULT, 'spread2');


INSERT INTO info.spreads_legs (instrument, execution_id) VALUES ('EDM22', 1);
INSERT INTO info.spreads_legs (instrument, execution_id) VALUES ('EDH23', 1);
INSERT INTO info.spreads_legs (instrument, execution_id) VALUES ('EDM22', 2);
INSERT INTO info.spreads_legs (instrument, execution_id) VALUES ('EDH23', 2);
INSERT INTO info.spreads_legs (instrument, execution_id) VALUES ('TUH23', 2);
INSERT INTO info.spreads_legs (instrument, execution_id) VALUES ('EDM22', 3);
INSERT INTO info.spreads_legs (instrument, execution_id) VALUES ('EDH23', 3);
INSERT INTO info.spreads_legs (instrument, execution_id) VALUES ('TUH23', 3);


INSERT INTO info.tsyyieldconstant (instrument, roll_constant) VALUES 
    ('EDH23',0.044522993),
    ('TUH23',0.120240538),
    ('FVSZ22',0.11189380);

INSERT INTO info.tag_products (tag, product, product_type) VALUES
    ('tag0','ED', 'Outright' ),
    ('tag1','UX', 'Outright' );

CREATE SCHEMA IF NOT EXISTS primarydata;


CREATE SCHEMA IF NOT EXISTS secondarydata;


CREATE TABLE IF NOT EXISTS primarydata.bonds_eod ( dtime DATE, bond text references info.bonds (isin),
                                                                                    close decimal, asset_swap_spread decimal, yield decimal, left_wing_yield decimal, right_wing_yield decimal, PRIMARY KEY (dtime,
                                                                                                                                                                                                             bond));


CREATE TABLE IF NOT EXISTS primarydata.base_eod
    ( dtime date, instrument text REFERENCES info.instruments ON DELETE RESTRICT ON UPDATE CASCADE,
                                                                                           open decimal, high decimal, low decimal, close decimal, settle decimal, netchange decimal, volume decimal, implied_volatiliy_3m decimal, openinterest decimal, current_dividend_yield decimal, forward_dividend_yield decimal, business_days_valuation smallint, PRIMARY KEY (dtime,
                                                                                                                                                                                                                                                                                                                                                                         instrument));

CREATE TABLE IF NOT EXISTS primarydata.eikon_opteod
    ( dtime date, instrument text,
        ask decimal, ask_implied_volatility decimal, bid decimal, bid_implied_volatility decimal, 
        delta decimal, gamma decimal, off_floor_volume decimal,  open_interest decimal, order_book_volume decimal, settle decimal, stock decimal, theta decimal, vega decimal);

CREATE TABLE IF NOT EXISTS primarydata.prodeod
    ( dtime date, product text, product_type text, aggregate_openinterest decimal,
     FOREIGN KEY (product,
                  product_type) REFERENCES info.products ON DELETE RESTRICT ON UPDATE CASCADE,
                                                                                      PRIMARY KEY (dtime,
                                                                                                   product,
                                                                                                   product_type));


CREATE TABLE IF NOT EXISTS primarydata.eikon_t2tquote
    ( dtime timestamptz,
      instrument text REFERENCES info.instruments ON DELETE RESTRICT ON UPDATE CASCADE,
                                                                               bid_price0 decimal, bid_size0 decimal, bid_orders0 int, ask_price0 decimal, ask_size0 decimal, ask_orders0 int, rectime timestamptz,
                                                                                                                                                                                               quote_id text, nanos int, PRIMARY KEY (dtime,
                                                                                                                                                                                                                                      instrument,
                                                                                                                                                                                                                                      quote_id));


CREATE TABLE IF NOT EXISTS primarydata.eikon_t2trade
    ( dtime timestamptz,
      instrument text REFERENCES info.instruments ON DELETE RESTRICT ON UPDATE CASCADE,
                                                                               trade_price decimal, trade_size decimal, aggressor text, rectime timestamptz,
                                                                                                                                        exch_trade_id text, nanos int, PRIMARY KEY (dtime,
                                                                                                                                                                                    instrument,
                                                                                                                                                                                    exch_trade_id));

select create_hypertable('primarydata.eikon_t2trade', 'dtime')  ;  
ALTER TABLE primarydata.eikon_t2trade SET (
	timescaledb.compress, 
	timescaledb.compress_orderby = 'exch_trade_id, dtime ASC', 
	timescaledb.compress_segmentby='instrument'
);                                                                                                                                                                  
-- SELECT add_compression_policy('primarydata.eikon_t2trade', INTERVAL '1d');

create table if not exists primarydata.refinitiv_t2tquote
(
    dtime timestamp,
    instrument text references info.instruments (instrument),
    bid_price0 decimal,
    bid_size0 decimal,
    bid_orders0 int,
    ask_price0 decimal,
    ask_size0 decimal,
    ask_orders0 int,
    rectime timestamp,
    quote_id text,
    nanos int,
    primary key (dtime, instrument, quote_id)
);

create table if not exists primarydata.refinitiv_t2trade
(
    dtime timestamp,
    instrument text references info.instruments (instrument),
    trade_price decimal,
    trade_size decimal,
    aggressor text,
    rectime timestamp,
    exch_trade_id text,
    nanos int,
    primary key (dtime, instrument, exch_trade_id)
);

CREATE TABLE IF NOT EXISTS primarydata.base_cheapest
(
    dtime date NOT NULL,
    instrument text  NOT NULL,
    cheapest text ,
    cheapest_fixed text
);


INSERT INTO primarydata.refinitiv_t2tquote (dtime, instrument, bid_price0, bid_size0, bid_orders0, ask_price0, ask_size0, ask_orders0, rectime, quote_id, nanos)
VALUES 
  ('2020-01-01 00:00:01', 'EDH23', 1.1, 100, 1, 1.2, 100, 1, NULL, '1', 0),
  ('2020-01-01 00:00:02', 'EDH23', 1.1, 100, 1, 1.2, 100, 1, NULL, '2', 0),
  ('2020-01-01 00:00:03', 'EDH23', 1.1, 100, 1, 1.2, 100, 1, NULL, '3', 0),
  ('2020-01-01 00:00:04', 'EDH23', 1.1, 100, 1, 1.2, 100, 1, NULL, '4', 0),
  ('2020-01-01 00:00:05', 'EDH23', 1.1, 100, 1, 1.2, 100, 1, NULL, '5', 0),
  ('2020-01-01 00:00:06', 'EDH23', 1.1, 100, 1, 1.2, 100, 1, NULL, '6', 0),
  ('2020-01-01 00:00:07', 'EDH23', 1.1, 100, 1, 1.2, 100, 1, NULL, '7', 0),
  ('2020-01-01 00:00:08', 'EDH23', 1.1, 100, 1, 1.2, 100, 1, NULL, '8', 0),
  ('2020-01-01 00:00:09', 'EDH23', 1.1, 100, 1, 1.2, 100, 1, NULL, '9', 0);


INSERT INTO primarydata.refinitiv_t2trade (dtime, instrument, trade_price, trade_size, aggressor, rectime, exch_trade_id, nanos)
VALUES 
  ('2020-01-01 00:00:01', 'EDH23', 1.1, 100, 'buy', NULL, '1', 0),
  ('2020-01-01 00:00:02', 'EDH23', 1.1, 100, 'buy', NULL, '2', 0),
  ('2020-01-01 00:00:03', 'EDH23', 1.1, 100, 'buy', NULL, '3', 0),
  ('2020-01-01 00:00:04', 'EDH23', 1.1, 100, 'buy', NULL, '4', 0),
  ('2020-01-01 00:00:05', 'EDH23', 1.1, 100, 'buy', NULL, '5', 0),
  ('2020-01-01 00:00:06', 'EDH23', 1.1, 100, 'buy', NULL, '6', 0),
  ('2020-01-01 00:00:07', 'EDH23', 1.1, 100, 'buy', NULL, '7', 0),
  ('2020-01-01 00:00:08', 'EDH23', 1.1, 100, 'buy', NULL, '8', 0),
  ('2020-01-01 00:00:09', 'EDH23', 1.1, 100, 'buy', NULL, '9', 0);

INSERT INTO primarydata.base_eod (dtime, instrument, open, high, low, close, settle, netchange, volume, implied_volatiliy_3m, openinterest, current_dividend_yield, forward_dividend_yield, business_days_valuation)
VALUES 
 ('2020-01-01', 'EDH23', 1.1, 1.1, 1.1, 1.1, 1.1, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
 ('2020-01-02', 'EDH23', 1.2, 1.2, 1.2, 1.2, 1.2, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
 ('2020-01-03', 'EDH23', 1.3, 1.3, 1.3, 1.3, 1.3, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
 ('2020-01-04', 'EDH23', 1.4, 1.4, 1.4, 1.4, 1.4, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
 ('2020-01-05', 'EDH23', 1.5, 1.5, 1.5, 1.5, 1.5, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
 ('2020-01-06', 'EDH23', 1.6, 1.6, 1.6, 1.6, 1.6, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
 ('2020-01-07', 'EDH23', 1.7, 1.7, 1.7, 1.7, 1.7, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
 ('2020-01-08', 'EDH23', 1.8, 1.8, 1.8, 1.8, 1.8, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
 ('2020-01-09', 'EDH23', 1.9, 1.9, 1.9, 1.9, 1.9, NULL, NULL, NULL, NULL, NULL, NULL, NULL);

 INSERT INTO primarydata.eikon_opteod (dtime, instrument, ask, ask_implied_volatility, bid, bid_implied_volatility, delta, gamma, off_floor_volume, open_interest, order_book_volume, settle, stock, theta, vega)
VALUES 
 ('2020-01-01', 'SX5EIDXC20230915M2850', 1.1, 1.1, 1.1, 1.1, 1.1, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
 ('2020-01-02', 'SX5EIDXC20230915M2850', 1.2, 1.2, 1.2, 1.2, 1.2, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
 ('2020-01-03', 'SX5EIDXC20230915M2850', 1.3, 1.3, 1.3, 1.3, 1.3, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
 ('2020-01-04', 'SX5EIDXC20230915D2850', 1.4, 1.4, 1.4, 1.4, 1.4, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
 ('2020-01-05', 'SX5EIDXC20230915W2850', 1.5, 1.5, 1.5, 1.5, 1.5, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
 ('2020-01-06', 'SX5EIDXP20230915W2850', 1.6, 1.6, 1.6, 1.6, 1.6, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
 ('2020-01-07', 'SX5EIDXP20230915W2850', 1.7, 1.7, 1.7, 1.7, 1.7, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
 ('2020-01-08', 'SX5EIDXC20230915M3000', 1.8, 1.8, 1.8, 1.8, 1.8, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
 ('2020-01-09', 'SX5EIDXC20230915M5000', 500, 1.9, 1.9, 1.9, 1.9, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);


INSERT INTO primarydata.base_cheapest (dtime, instrument, cheapest, cheapest_fixed) VALUES 
('2023-02-16','TUH23','US91282CGD74','US912828YY08'),
('2023-02-15','TUH23','US912828YV68','US912828YV68'),
('2023-02-14','TUH23','olderValue','olderValue_fixed');


create or replace view primarydata.base_opteod
as 
select 
	dtime,
	instrument,
	matches[1] AS underlayer,
  	matches[2] AS direction,
  	 TO_DATE(matches[3], 'YYYYMMDD') AS maturity,
  	matches[4] AS type,
  	CAST(matches[5] AS decimal) AS strike,
  	ask,
  	ask_implied_volatility,
  	bid,
  	bid_implied_volatility,
  	delta,
  	gamma,
  	off_floor_volume,
  	open_interest,
  	order_book_volume,
  	settle,
  	stock,
  	theta,
  	vega
  from (
		select 
		*, 
		regexp_matches(instrument, '^([A-Z0-9]+)([CP])([0-9]+)([DWMEY])([0-9]+)$') AS matches
		from primarydata.eikon_opteod t
	) sub;


	create table secondarydata.cus_indexes_eod (like primarydata.base_eod);
  ALTER TABLE secondarydata.cus_indexes_eod
DROP CONSTRAINT IF EXISTS dtime_instrument_pkey,
ADD CONSTRAINT dtime_instrument_pkey PRIMARY KEY (dtime, instrument);


CREATE TABLE IF NOT EXISTS secondarydata.base_strategieseod
(
    dtime date,
    strategy text ,
    yield_to_mty numeric,
    settle numeric,
    generic_strategy text ,
    seasonal_adjusted boolean,
    PRIMARY KEY (dtime, strategy)
);


CREATE TABLE IF NOT EXISTS secondarydata.base_strategiesintra
(
    dtime timestamptz,
    strategy text ,
    yield_to_mty numeric,
    mid_close numeric,
    generic_strategy text ,
    seasonal_adjusted boolean,
    PRIMARY KEY (dtime, strategy)
);


CREATE TABLE IF NOT EXISTS secondarydata.base_spreadeod
(
    dtime date NOT NULL,
    spread text  NOT NULL,
    yield_to_mty Decimal,
    cmt_yield Decimal
);

CREATE TABLE IF NOT EXISTS secondarydata.base_spreadintra
(
    dtime date NOT NULL,
    spread text  NOT NULL,
    yield_to_mty Decimal,
    cmt_yield Decimal
);


CREATE OR REPLACE FUNCTION primarydata.order_tickers(name text) RETURNS text
    LANGUAGE plpgsql
    AS $_$/*
D
  Function: order_instruments
  -----------------------------
  Generates a sort key for instrument names.

  First, it checks against a set of regex patterns (defined in parsedregex) and, in the TDX case,
  uses a tenor conversion to build a sort key. Otherwise, it then checks if the instrument ends with
  an expiration substring matching the pattern [FGHJKMNQUVXZ]\d\d. If it does, the function extracts the
  base (name without expiration), maps the letter to a month, converts the two-digit year (offset from 2000),
  and returns a key in the format: base || YYYYMM.
  
  Parameters:
    name - the original instrument name

  Returns:
    A text sort key for the instrument.
  
  Note:
    This function is meant to be used as part of your ORDER BY clause to enforce the desired sorting.
*/
DECLARE
	parsedregex CONSTANT JSONB := '{
        "FSW": "(\\w+)F(\\d+)([DWMY])(\\d+)([DWMY])SW",
        "GNR": "(\\w+)G(?:\\d+|\\w)NR",
        "IDX": "(\\w+)IDX",
        "PDX": "(EC\\w+)P(Q[1-4])?(\\d{4})DX",
        "TDX": "(\\w+)T(\\d+[DWMY](?:_\\d+[DWMY])?)DX",
        "TGN": "(\\w+)T(\\d+[DWMY](?:_\\d+[DWMY])?)(?:\\d+|\\w)GN"
    }'::JSONB;
	kind TEXT;
	reggroups TEXT[];
	mixtenor TEXT;
	-- expiration processing variables
	exp_pattern CONSTANT TEXT := '^[FGHJKMNQUVXZ]\d\d$';
	base TEXT;
	exp TEXT;
	letter CHAR;
	digits TEXT;
	year INT;
	month INT;
	formatted_exp TEXT;
BEGIN
	-- Loop through defined regex patterns.
	FOR kind IN SELECT jsonb_object_keys(parsedregex)
	LOOP
		IF name ~ (parsedregex->>kind) THEN
			reggroups := regexp_match(name, (parsedregex->>kind));
			RAISE NOTICE 'Matched groups: %', reggroups;
			CASE kind
			WHEN 'TDX' THEN 
				mixtenor := LPAD(tenor_to_days(reggroups[2])::TEXT, 5, '0');
				RETURN reggroups[1] || kind || mixtenor;
			END CASE;
			RETURN kind;
		END IF;
	END LOOP;

	-- If no regex pattern matched, check for a valid expiration at the end.
	IF char_length(name) >= 3 THEN
		exp := right(name, 3);
		IF exp ~ exp_pattern THEN
			base := left(name, char_length(name) - 3);
			letter := left(exp, 1);
			digits := right(exp, 2);
			year := 2000 + digits::INT;
			month := CASE letter
						WHEN 'F' THEN 1
						WHEN 'G' THEN 2
						WHEN 'H' THEN 3
						WHEN 'J' THEN 4
						WHEN 'K' THEN 5
						WHEN 'M' THEN 6
						WHEN 'N' THEN 7
						WHEN 'Q' THEN 8
						WHEN 'U' THEN 9
						WHEN 'V' THEN 10
						WHEN 'X' THEN 11
						WHEN 'Z' THEN 12
						ELSE 0
					 END;
			-- Format as YYYYMM to ensure proper lexicographical order.
			formatted_exp := LPAD(year::TEXT, 4, '0') || LPAD(month::TEXT, 2, '0');
			RETURN base || formatted_exp;
		END IF;
	END IF;
	RETURN name;
END;
$_$;


-- VIEWS


CREATE OR REPLACE VIEW info.strategy_names as
 SELECT DISTINCT strats.strat AS strategy_name
   FROM ( SELECT DISTINCT base_strategieseod.strategy AS strat
           FROM secondarydata.base_strategieseod
        UNION
         SELECT DISTINCT base_spreadintra.spread AS strat
           FROM secondarydata.base_spreadintra
        UNION
         SELECT DISTINCT base_spreadeod.spread AS strat
           FROM secondarydata.base_spreadeod) strats;


CREATE OR REPLACE VIEW info.tags_view AS
WITH      prods AS (
          SELECT    tag,
                    jsonb_agg(jsonb_build_object('product', product, 'product_type', product_type)) AS products
          FROM      info.tag_products
          GROUP BY  tag
          ),
          instrs AS (
          SELECT    tag,
                    array_agg(instrument) AS instruments
          FROM      info.tag_instruments
          GROUP BY  tag
          ),
          filters AS (
          SELECT    tag,
                    array_agg(strategy_filter) AS strategy_filters
          FROM      info.tag_strategy_filters
          GROUP BY  tag
          ),
          strats AS (
          SELECT    ordst.tag,
                    array_agg(DISTINCT ordst.strategies) AS strategies
          FROM      (
                    SELECT    tsf.tag,
                              sn.strategy_name AS strategies
                    FROM      (
                              info.tag_strategy_filters tsf
                    JOIN      info.strategy_names sn ON lower(sn.strategy_name) ~* tsf.strategy_filter
                              )
                    ORDER BY  (primarydata.order_tickers (sn.strategy_name))
                    ) ordst
          GROUP BY  ordst.tag
          ),
          custom_filters AS (
          SELECT    tag,
                    array_agg(custom_instrument_filter) AS custom_instrument_filters
          FROM      info.tag_custom_filters
          GROUP BY  tag
          ),
          custom_instrs AS (
          SELECT    ordcis.tag,
                    array_agg(DISTINCT ordcis.custom_instruments) AS custom_instruments
          FROM      (
                    SELECT    tcf.tag,
                              ci.custom_index AS custom_instruments
                    FROM      info.tag_custom_filters tcf
                    JOIN      info.custom_indices ci ON lower(ci.custom_index) ~* tcf.custom_instrument_filter
                    ORDER BY  primarydata.order_tickers (ci.custom_index)
                    ) ordcis
          GROUP BY  ordcis.tag
          ),
          inh AS (
          SELECT    tag,
                    array_agg(
                    instrument
                    ORDER BY  instrument
                    ) AS inherited_instruments
          FROM      (
                    -- both direct and product-derived instruments
                    SELECT    tag,
                              instrument
                    FROM      info.tag_instruments
                    UNION    
                    SELECT    itp.tag,
                              inst.instrument
                    FROM      info.tag_products itp
                    JOIN      info.instruments inst ON inst.product = itp.product
                    AND       inst.product_type = itp.product_type
                    ) u
          GROUP BY  tag
          )
SELECT    t.tag,
          p.products,
          i.instruments,
          f.strategy_filters,
          s.strategies,
          cf.custom_instrument_filters,
          cis.custom_instruments,
          inh.inherited_instruments
          FROM info.tags t
LEFT JOIN prods p USING (tag)
LEFT JOIN instrs i USING (tag)
LEFT JOIN filters f USING (tag)
LEFT JOIN strats s USING (tag)
LEFT JOIN custom_filters cf USING (tag)
LEFT JOIN custom_instrs cis USING (tag)
LEFT JOIN inh USING (tag)
ORDER BY tag ASC;

CREATE OR REPLACE VIEW info.instrument_tags AS
 SELECT
  sub.instrument,
  array_agg(DISTINCT tv.tag)   AS tags
FROM info.tags_view tv
CROSS JOIN LATERAL
  unnest(tv.inherited_instruments || tv.instruments) AS sub(instrument)
GROUP BY sub.instrument;

CREATE OR REPLACE VIEW info.instrument_last_cheapest as
 SELECT DISTINCT ON (base.instrument) base.dtime,
    base.instrument,
    bonds.isin,
    bonds.bond_name,
    bonds.cusip,
    bonds.coupon,
    bonds.maturity,
    bonds.left_wing,
    bonds.right_wing,
    bonds.issue_date,
    bonds.issuer,
    bonds.coupon_type,
    bonds.coupon_formula,
    bonds.day_count_convention,
    bonds.ammount_issued,
    bonds.ammount_outstanding,
    bonds.first_accrual_date,
    bonds.first_coupon_date,
    bonds.country_of_risk,
    bonds.currency,
    bonds.coupon_freq,
    deliv.conversion_factor,
    deliv.has_been_cheapest,
    inst.product,
    inst.product_type,
    inst.first_tradeable_date,
    inst.last_tradeable_date,
    inst.first_delivery_date,
    inst.last_delivery_date,
    inst.underlying_coupon,
    inst.description,
    inst.refinitiv_ticker,
    inst.bloomberg_ticker,
    inst.bloomberg_suffix,
    yieldcst.roll_constant,
    itags.tags
   FROM  primarydata.base_cheapest base
     JOIN  info.bonds bonds ON base.cheapest = bonds.isin
     LEFT JOIN  info.deliverables deliv ON
      deliv.deliverable_isin = bonds.isin 
      AND deliv.instrument = base.instrument
     LEFT JOIN  info.instruments inst ON base.instrument = inst.instrument
     LEFT JOIN  info.tsyyieldconstant yieldcst ON inst.instrument = yieldcst.instrument
     LEFT JOIN  info.instrument_tags itags ON inst.instrument = itags.instrument
  WHERE base.cheapest IS NOT NULL
  ORDER BY base.instrument, base.dtime DESC;

CREATE OR REPLACE VIEW info.instrument_cheapest_fixed as
SELECT DISTINCT ON (base.instrument) base.dtime,
    base.instrument,
    bonds.isin,
    bonds.bond_name,
    bonds.cusip,
    bonds.coupon,
    bonds.maturity,
    bonds.left_wing,
    bonds.right_wing,
    bonds.issue_date,
    bonds.issuer,
    bonds.coupon_type,
    bonds.coupon_formula,
    bonds.day_count_convention,
    bonds.ammount_issued,
    bonds.ammount_outstanding,
    bonds.first_accrual_date,
    bonds.first_coupon_date,
    bonds.country_of_risk,
    bonds.currency,
    bonds.coupon_freq,
    deliv.conversion_factor,
    deliv.has_been_cheapest,
    inst.product,
    inst.product_type,
    inst.first_tradeable_date,
    inst.last_tradeable_date,
    inst.first_delivery_date,
    inst.last_delivery_date,
    inst.underlying_coupon,
    inst.description,
    inst.refinitiv_ticker,
    inst.bloomberg_ticker,
    inst.bloomberg_suffix,
    yieldcst.roll_constant,
    itags.tags
   FROM primarydata.base_cheapest base
     JOIN  info.bonds bonds ON base.cheapest = bonds.isin
     LEFT JOIN  info.deliverables deliv ON
      deliv.deliverable_isin = bonds.isin 
      AND deliv.instrument = base.instrument
     LEFT JOIN  info.instruments inst ON base.instrument = inst.instrument
     LEFT JOIN  info.tsyyieldconstant yieldcst ON inst.instrument = yieldcst.instrument
     LEFT JOIN  info.instrument_tags itags ON inst.instrument = itags.instrument
  WHERE base.cheapest_fixed IS NOT NULL
  ORDER BY base.instrument, base.dtime DESC;

CREATE OR REPLACE VIEW info.complete_instruments
 AS
 SELECT inst.instrument,
    inst.product,
    inst.product_type,
    inst.first_tradeable_date,
    inst.last_tradeable_date,
    inst.first_delivery_date,
    inst.last_delivery_date,
    inst.underlying_coupon,
    COALESCE(inst.description,products.description) as description,
    inst.refinitiv_ticker,
    inst.bloomberg_ticker,
    inst.bloomberg_suffix,
    inst.column_fields_override,
	inst.seasonal_factor_close,
	inst.seasonal_factor_early_close,
    yieldcst.roll_constant,
    itags.tags
   FROM info.instruments inst
     LEFT JOIN info.tsyyieldconstant yieldcst ON inst.instrument = yieldcst.instrument
     LEFT JOIN info.instrument_tags itags ON inst.instrument = itags.instrument
	 LEFT JOIN info.products ON products.product = inst.product AND products.product_type = inst.product_type
  ORDER BY inst.instrument;