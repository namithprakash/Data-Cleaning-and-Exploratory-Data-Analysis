-- MySQL Data Cleaning Project:

	-- Setting 'world_layoffs' as the Current DB:
    USE world_layoffs;
    
	-- Selecting everything from the table 'layoffs':
    SELECT * 
    FROM layoffs;
    
    -- REMOVING DUPLICATES:
    -- Creating a table with the same data as 'layoffs':
    -- This table is created so that the data on the original/raw table is preserved in case of any mistakes
    CREATE TABLE layoffs_staging
    LIKE layoffs;
    
    -- Selecting everything from 'layoffs_staging' to see if table was created successfully:
    SELECT * 
    FROM layoffs_staging;
    
    -- Inserting the data from table 'layoffs' into table 'layoffs_staging':
    INSERT layoffs_staging
	SELECT * 
    FROM layoffs;
    
    -- Selecting everything from 'layoffs_staging' to see if table has all the data from 'layoffs':
    SELECT * 
    FROM layoffs_staging;
    
    -- Creating a column 'row_num' that displays the unique rows as a number:
    SELECT *,
    ROw_NUMBER() 
		OVER(PARTITION BY 
			company,
            location,
            industry,
            total_laid_off,
            percentage_laid_off,
            'date',
            stage,
            country,
            funds_raised_millions) AS row_num
	FROM layoffs_staging;
    
    -- Using the 'row_num' column as a reference to select the duplicate rows from 'layoff_staging':
    WITH dupe_cte AS
    (
		 SELECT *,
		 ROw_NUMBER() 
		 OVER(PARTITION BY 
			 company,
             location,
             industry,
             total_laid_off,
             percentage_laid_off,
             'date',
             stage,
             country,
             funds_raised_millions) AS row_num
		 FROM layoffs_staging	
    )
    SELECT *
    FROM dupe_cte
    WHERE row_num > 1;
    
    -- Checking to see if any of the selected duplicate data are actual duplicates
	SELECT *
    FROM layoffs_staging
    WHERE company = 'ExtraHop';
    
    
    -- Creating a table 'layoffs_staging2' that will be used to delete the duplicates:
    CREATE TABLE `layoffs_staging2` 
    (
	  `company` text,
	  `location` text,
	  `industry` text,
	  `total_laid_off` int DEFAULT NULL,
	  `percentage_laid_off` text,
	  `date` text,
	  `stage` text,
	  `country` text,
	  `funds_raised_millions` int DEFAULT NULL,
      `row_num` int
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
    
    -- Checking to see if table 'layoffs_staging2' was created successfully:
    SELECT *
    FROM layoffs_staging2;

	-- Inserting the table data with the new column 'row_num' into table 'layoffs_staging2':
	INSERT INTO layoffs_staging2
    SELECT *,
		 ROw_NUMBER() 
		 OVER(PARTITION BY 
			 company,
             location,
             industry,
             total_laid_off,
             percentage_laid_off,
             'date',
             stage,
             country,
             funds_raised_millions) AS row_num
		 FROM layoffs_staging;
         
	-- Checking to see if the data was pasted to table 'layoffs_staging2':
    SELECT *
    FROM layoffs_staging2;
    
    -- Deleting the duplicates from 'layoffs_staging2':
    DELETE
    FROM layoffs_staging2
    WHERE row_num > 1;
    
    -- Checking to see if the duplicates were deleted:
    SELECT *
    FROM layoffs_staging2
    WHERE row_num > 1;
    
    -- STANDARDIZING DATA
    
    -- Checking the column 'industry'
    SELECT DISTINCT company
    FROM layoffs_staging2;
    
    -- Removing the spaces on the left side of all the values in column 'company': 
    UPDATE layoffs_staging2
    SET company = TRIM(company);
    
	-- Checking to see if the TRIM was updated on 'company':
    SELECT company
    FROM layoffs_staging2;
    
    -- Checking the column 'industry':
    SELECT DISTINCT industry
    FROM layoffs_staging2
    ORDER BY 1;
    
    -- Found some irregularities with the 'Crypto' as industry:
    SELECT *
    FROM layoffs_staging2
    WHERE industry LIKE 'Crypto%';
    
    -- Fixing irregular names of Crypto to 'Crypto':
	UPDATE layoffs_staging2	
    SET industry = 'Crypto'
    WHERE industry LIKE 'Crypto%';
    
    -- Checking to see the values in 'location':
    SELECT DISTINCT location
    FROM layoffs_staging2
    ORDER BY 1;
    
    -- Checking to see the values in 'country':
    SELECT DISTINCT country
    FROM layoffs_staging2
    ORDER BY 1;
    
    -- Updating the country name by removing irregularities:
    UPDATE layoffs_staging2	
    SET country = 'United States'
    WHERE country LIKE 'United States%';
    
    -- Looking at the values in the date column:
    SELECT `date`
    FROM layoffs_staging2;
    
    -- Updating the format of the date column:
    UPDATE layoffs_staging2
    SET `date` =  str_to_date(`date`,'%m/%d/%Y');
    
    -- ALtering the datatype of the date column from text to date:
    ALTER TABLE layoffs_staging2
    MODIFY COLUMN `date` DATE;
    
    -- Selecting all the rows where the industry value is null or blank:
    SELECT *
    FROM layoffs_staging2
    WHERE industry IS NULL
    OR industry = '';
    
    -- Taking Airbnb as an example:
    SELECT *
    FROM layoffs_staging2
    WHERE company = 'Airbnb';
    
    -- Using join to see the company with and without industry as null:
	SELECT t1.industry, t2.industry
    FROM layoffs_staging2 t1
    JOIN layoffs_staging2 t2
		ON t1.company = t2.company
        AND t1.location = t2.location
	WHERE (t1.industry IS NULL OR t1.industry = '')
    AND t2.industry IS NOT NULL;
    
    -- Updating all the blank values to null in the industry column: 
    UPDATE layoffs_staging2
    SET industry = NULL
    WHERE industry = '';
    
    -- Copying all the data where company has industry to the rows where industry of company is null:
    UPDATE layoffs_staging2 t1
    JOIN layoffs_staging2 t2
		ON t1.company = t2.company
	SET t1.industry = t2.industry
    WHERE (t1.industry IS NULL)
    AND t2.industry IS NOT NULL;

	-- Selecting all the rows where values for total_laid_off and percentage_laid_off are null:
	SELECT *
    FROM layoffs_staging2
    WHERE total_laid_off IS NULL
    AND percentage_laid_off IS NULL;
    
    -- Deleting all the rows where values for total_laid_off and percentage_laid_off are null:
	DELETE
    FROM layoffs_staging2
    WHERE total_laid_off IS NULL
    AND percentage_laid_off IS NULL;
    
    -- Checking to see if they were deleted successfully:
    SELECT *
    FROM layoffs_staging2;
 
	-- Deleting the column row_num as it is no longer required:
	ALTER TABLE layoffs_staging2
    DROP COLUMN row_num;
 
	-- Checking to see if the column was successfully deleted:
	SELECT *
    FROM layoffs_staging2;
    