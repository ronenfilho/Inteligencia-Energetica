
    
    

select
    id_dim_localidade as unique_field,
    count(*) as n_records

from IE_DB.CORE.dim_localidade
where id_dim_localidade is not null
group by id_dim_localidade
having count(*) > 1


