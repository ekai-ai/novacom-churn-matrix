select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select service_id
from NOVACOM.KPIS.service_adoption_rate
where service_id is null



      
    ) dbt_internal_test