with source as (

    select * from read_csv_auto('../data/raw/users.csv')

),

cleaned as (

    select
        user_id,
        name,
        email,
        country,
        -- Normalize empty strings to null
        nullif(trim(city), '') as city,
        platform,

        -- Parse signup_date from three possible formats:
        --   YYYY-MM-DD, DD-MM-YYYY, DD/MM/YYYY
        case
            when signup_date ~ '^\d{4}-\d{2}-\d{2}$'
                then cast(signup_date as date)
            when signup_date ~ '^\d{2}-\d{2}-\d{4}$'
                then strptime(signup_date, '%d-%m-%Y')::date
            when signup_date ~ '^\d{2}/\d{2}/\d{4}$'
                then strptime(signup_date, '%d/%m/%Y')::date
            else null
        end as signup_date,

        subscription_type,
        age,

        -- Standardize gender values
        case lower(trim(gender))
            when 'm' then 'male'
            when 'male' then 'male'
            when 'f' then 'female'
            when 'female' then 'female'
            when 'o' then 'other'
            when 'other' then 'other'
            else 'unknown'
        end as gender

    from source

)

select * from cleaned
