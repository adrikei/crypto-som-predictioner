CREATE TABLE btcusdt (
    id integer NOT NULL,
    price numeric,
    quantity numeric,
    ts timestamp with time zone,
    buyer_maker boolean,
    best boolean
);

ALTER TABLE btcusdt ADD CONSTRAINT btcusdt_pkey PRIMARY KEY (id);

-- bitcoin usdt _ aggregated time 10
create table bu_sa_t10(
    ts timestamp primary key,
    buy_trades int,
    buy_sum float,
    buy_avg_w_price float,
    sell_trades int,
    sell_sum float,
    sell_avg_w_price float,
    wavg float
);

create or replace view bu_g10 as (
    select 1+max(id)-min(id) as trades, sum(quantity), sum(price*quantity)/(sum(quantity)) avg_price, 
        case when buyer_maker=TRUE then 'sell' else 'buy' end B_S, 
        to_timestamp(((extract(epoch from ts)/(10))::int)*(10)) t_s from btcusdt group by t_s, buyer_maker
);

-- now insert the data into btcusdt
-- $ python database_filler.py BTCUSDT > btcusdt.sql
-- $ psql -f btcusdt.sql

insert into bu_sa_t10(ts) select distinct t_s from bu_g10;
    update bu_sa_t10
        set buy_trades = q.trades,
            buy_sum = q.sum,
            buy_avg_w_price = q.avg_price
        from        
            (select * from bu_g10 where b_s = 'buy') q
        where q.t_s = bu_sa_t10.ts;

    update bu_sa_t10
        set sell_trades = q.trades,
            sell_sum = q.sum,
            sell_avg_w_price = q.avg_price
        from
            (select * from bu_g10 where b_s = 'sell') q
        where q.t_s = bu_sa_t10.ts;

    update bu_sa_t10
        set wavg = (buy_avg_w_price * buy_sum + sell_avg_w_price * sell_sum)/(buy_sum + sell_sum);

    update bu_sa_t10
        set wavg = buy_avg_w_price
        where wavg is null;

    update bu_sa_t10
        set wavg = sell_avg_w_price
        where wavg is null;

    update bu_sa_t10
        set buy_trades = 0,
        buy_sum = 0
        where buy_trades is null;

    update bu_sa_t10
        set sell_trades = 0,
        sell_sum = 0
        where sell_trades is null;

-- should run this until the number of updated rows stagnate
update bu_sa_t10 o
    set buy_avg_w_price = (select i.buy_avg_w_price from bu_sa_t10 i where i.ts = (o.ts - interval '10 seconds'))
    where o.buy_avg_w_price is null;

-- should run this once and then the 10 seconds one until the number of updated rows stagnate
update bu_sa_t10 o
    set buy_avg_w_price = (select i.buy_avg_w_price from bu_sa_t10 i where i.ts = (o.ts - interval '20 seconds'))
    where o.buy_avg_w_price is null;

-- should run this once and then the 10 seconds one until the number of updated rows stagnate
update bu_sa_t10 o
    set sell_avg_w_price = (select i.sell_avg_w_price from bu_sa_t10 i where i.ts = (o.ts - interval '30 seconds'))
    where o.sell_avg_w_price is null;
