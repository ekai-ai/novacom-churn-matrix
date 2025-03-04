select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select customers_using_service
from NOVACOM.KPIS.service_adoption_rate
where customers_using_service is null



      
    ) dbt_internal_test