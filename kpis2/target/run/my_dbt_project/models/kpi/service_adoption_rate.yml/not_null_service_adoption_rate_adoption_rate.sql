select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select adoption_rate
from NOVACOM.KPIS.service_adoption_rate
where adoption_rate is null



      
    ) dbt_internal_test