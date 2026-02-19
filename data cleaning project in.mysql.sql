-- DATA CLEANING 

-- Removig duplicates
-- Standardizing the data
-- null values and blank values
-- removing any column 



create table layoff_staging 
like layoffs;

select *
 from  layoff_staging;
 
 insert layoff_staging 
 select * from layoffs;
 
 select *, 
 ROW_NUMBER() OVER( partition by company, location, industry, total_laid_off, 
 percentage_laid_off, `date`, stage, country, funds_raised_millions) as row_num
 from layoff_staging;
 
 with duplicate_cte as (
 select *, 
 ROW_NUMBER() OVER( partition by company, location, industry, total_laid_off, 
 percentage_laid_off, `date`, stage, country, funds_raised_millions) as row_num
 from layoff_staging
 )
select * 
from duplicate_cte 
where row_num > 1;

select *
 from  layoff_staging
 where company = 'casper'
 ;
 
 with duplicate_cte as (
 select *, 
 ROW_NUMBER() OVER( partition by company, location, industry, total_laid_off, 
 percentage_laid_off, `date`, stage, country, funds_raised_millions) as row_num
 from layoff_staging
 )
delete 
from duplicate_cte 
where row_num > 1;

CREATE TABLE `world_layoffs`.`layoffs_staging2` (
`company` text,
`location`text,
`industry`text,
`total_laid_off` INT,
`percentage_laid_off` text,
`date` text,
`stage`text,
`country` text,
`funds_raised_millions` int,
row_num INT
);

select * from layoffs_staging2;

insert into layoffs_staging2
select *,
ROW_NUMBER() OVER( partition by company, location, industry, total_laid_off, 
 percentage_laid_off, `date`, stage, country, funds_raised_millions) as row_num
 from layoff_staging;
 
 select * from layoffs_staging2
 where row_num > 1;

SET SQL_SAFE_UPDATES = 0;

delete 
from layoffs_staging2
where row_num > 1;

select company, trim(company) 
from layoffs_staging2;

update layoffs_staging2
set company = trim(company);

select  *
from layoffs_staging2
where industry like 'crypto%';

update layoffs_staging2
set industry = 'crypto'
where industry like 'crypto%';

select  country
from layoffs_staging2
order by 1;

select distinct country, trim(trailing '.' from country)
from layoffs_staging2
order by 1;

 update layoffs_staging2 
 set country = trim(trailing '.' from country)
 where country like 'united states%'
 ;
 
 select `date`,
 str_to_date(`date`, '%m/%d/%Y')
 from layoffs_staging2;
 
 update layoffs_staging2
 set `date` = str_to_date(`date`, '%m/%d/%Y')
 ;
 
 select `date`
 from layoffs_staging2;
 
 alter table layoffs_staging2
 modify column `date` date;
 
 select *
 from layoffs_staging2
 where total_laid_off is null
 and percentage_laid_off is null;
 
 select *
 from layoffs_staging2
 where industry is null
 or industry = '';
 
 update layoffs_staging2 
 set industry = null
 where industry = '';
 
 select * 
 from layoffs_staging2
 where company = 'airbnb';
 
 select t1.industry, t2.industry
 from layoffs_staging2 t1
 join layoffs_staging2 t2
  on t1.company = t2.company
   where (t1.industry is null or t1.industry = '')
   AND t2.industry is not null;
   
   update layoffs_staging2 t1
    join layoffs_staging2 t2
  on t1.company = t2.company
  set t1.industry = t2.industry
  where  t1.industry is null 
   AND t2.industry is not null;
 
 
 select *
 from layoffs_staging2
 where company like 'bally%';

select *
 from layoffs_staging2
 where total_laid_off is null
 and percentage_laid_off is null;
 
 -- i cant trust the data so i am just deleating the null rows of total laid off and percentage laid off. 
 delete 
 from layoffs_staging2
 where total_laid_off is null
 and percentage_laid_off is null;
 
 select * 
 from layoffs_staging2;
 
 alter table layoffs_staging2
 drop column row_num
 ;

