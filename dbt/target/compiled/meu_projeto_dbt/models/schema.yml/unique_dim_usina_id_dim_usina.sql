
    
    

select
    id_dim_usina as unique_field,
    count(*) as n_records

from IE_DB.CORE.dim_usina
where id_dim_usina is not null
group by id_dim_usina
having count(*) > 1


