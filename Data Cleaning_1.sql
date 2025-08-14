SELECT * 
FROM layoffs;

-- 1. Remove Duplicates
-- 2. Standardize the Data
-- 3. Null Values or blank values
-- 4. Remove any columns and rows

CREATE TABLE layoffs_staging2
LIKE layoffs;

SELECT *
FROM layoffs_staging2;

INSERT layoffs_staging2
SELECT *
FROM layoffs;

SELECT *,
ROW_NUMBER() OVER(PARTITION BY company,location,industry,total_laid_off,percentage_laid_off,'date',stage,country,funds_raised_millions)
AS row_num
FROM layoffs_staging2;

-- CREATE A TEMPORARY TABLE
WITH duplicate_CTE AS
(SELECT *,
ROW_NUMBER() OVER(PARTITION BY company,location,industry,total_laid_off,percentage_laid_off,'date',stage,country,funds_raised_millions)
AS row_num
FROM layoffs_staging2
)
SELECT *
FROM duplicate_CTE
WHERE row_num > 1;

SELECT *
FROM layoffs_staging2
WHERE company = 'Casper';

CREATE TABLE `layoffs_staging3` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SELECT *
FROM layoffs_staging3;

INSERT INTO layoffs_staging3
SELECT *,
ROW_NUMBER() OVER(PARTITION BY company,location,industry,total_laid_off,percentage_laid_off,'date',stage,country,funds_raised_millions)
AS row_num
FROM layoffs_staging2;

DELETE
FROM layoffs_staging3
WHERE row_num >1;

SELECT *
FROM layoffs_staging3
WHERE row_num >1;

SELECT *
FROM layoffs_staging3;

-- 2. Standardize the Data
-- Trim take wide spaces from

SELECT company, (TRIM(company))
FROM layoffs_staging3;

UPDATE layoffs_staging3
SET company =TRIM(company);

SELECT DISTINCT(industry)
FROM layoffs_staging3
ORDER BY 1;

SELECT *
FROM layoffs_staging3
WHERE industry LIKE 'Crypto%';

UPDATE layoffs_staging3
SET industry ='crypto'
WHERE industry LIKE 'Crypto%';

SELECT DISTINCT(country)
FROM layoffs_staging3
ORDER BY 1;

SELECT*
FROM layoffs_staging3
WHERE country LIKE 'United States%';

SELECT DISTINCT country,TRIM(TRAILING '.' FROM country)
FROM layoffs_staging3
ORDER BY 1;

UPDATE layoffs_staging3
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';

SELECT `date`,
STR_TO_DATE (`date`,'%m/%d/%Y')
FROM layoffs_staging3;

UPDATE layoffs_staging3
SET `date` = STR_TO_DATE (`date`,'%m/%d/%Y');

SELECT `date`
FROM layoffs_staging3;

ALTER TABLE layoffs_staging3
MODIFY COLUMN `date` DATE;

SELECT *
FROM layoffs_staging3;

-- 3. Null Values or blank values

SELECT *
FROM layoffs_staging3
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

UPDATE layoffs_staging3
SET industry = NULL
WHERE industry = '';

SELECT *
FROM layoffs_staging3
WHERE industry IS NULL
OR industry ='';

SELECT *
FROM layoffs_staging3
WHERE company ='Airbnb';

SELECT t1.industry, t2.industry
FROM layoffs_staging3 t1
JOIN layoffs_staging3 t2
	ON t1.company = t2.company
    AND t1.location = t2.location
WHERE t1.industry IS NULL OR t1.industry=''
AND t2.industry IS NOT NULL;

UPDATE layoffs_staging3 t1
JOIN layoffs_staging3 t2
	ON t1.company = t2.company
SET t1.industry = t2.industry 
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;

SELECT *
FROM layoffs_staging3
WHERE company ="Bally's Interactive";

-- 4. Remove any columns

SELECT *
FROM layoffs_staging3
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

DELETE
FROM layoffs_staging3
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT *
FROM layoffs_staging3;

ALTER TABLE layoffs_staging3
DROP COLUMN row_num;