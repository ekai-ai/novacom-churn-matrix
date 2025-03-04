select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select total_customers
from NOVACOM.KPIS.service_adoption_rate
where total_customers is null



      
    ) dbt_internal_test