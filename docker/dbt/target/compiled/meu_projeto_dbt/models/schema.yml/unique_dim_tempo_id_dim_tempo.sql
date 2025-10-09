
    
    

select
    id_dim_tempo as unique_field,
    count(*) as n_records

from LAB_PIPELINE.CORE.dim_tempo
where id_dim_tempo is not null
group by id_dim_tempo
having count(*) > 1


