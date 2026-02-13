/*******************************************************************************
Vertical integration and product variety - Process publishers
Created: August, 2025
Author: Felipe Del Canto
*******************************************************************************/

/*
	
	
	
*/

*******************************************************************************
** Create folder for table
*******************************************************************************

!mkdir "tables/summary statistics"

*******************************************************************************
** Prepare developer data
*******************************************************************************

** Open data
use "data/processed_developer_data.dta", clear

** Keep only one observation per game
duplicates drop app_id, force

** Tag developers
egen developer_tag = tag(developer)
ereplace inSample = tag(developer) if inSample

*******************************************************************************
** Summary statistics
*******************************************************************************

** Number of games by developer
egen num_games = count(name), by(developer)

egen ss_avg_Games = mean(num_games) if developer_tag
egen ss_sd_Games = sd(num_games) if developer_tag
egen ss_min_Games = min(num_games) if developer_tag
egen ss_max_Games = max(num_games) if developer_tag

egen ss_avg_Games_inSample = mean(num_games) if inSample & developer_tag
egen ss_sd_Games_inSample = sd(num_games) if inSample & developer_tag
egen ss_min_Games_inSample = min(num_games) if inSample & developer_tag
egen ss_max_Games_inSample = max(num_games) if inSample & developer_tag

drop num_games

** Number of positive/negative reviews by developer
egen PosReviews = total(positive_reviews / 1000), by(developer)
egen NegReviews = total(negative_reviews / 1000), by(developer)

foreach prefix in Pos Neg {
	replace `prefix'Reviews = . if `prefix'Reviews == 0

	egen ss_avg_`prefix'Reviews = mean(`prefix'Reviews) if developer_tag
	egen ss_sd_`prefix'Reviews  = sd(`prefix'Reviews) if developer_tag
	egen ss_min_`prefix'Reviews = min(`prefix'Reviews) if developer_tag
	egen ss_max_`prefix'Reviews = max(`prefix'Reviews) if developer_tag

	egen ss_avg_`prefix'Reviews_inSample = mean(`prefix'Reviews) if inSample & developer_tag
	egen ss_sd_`prefix'Reviews_inSample  = sd(`prefix'Reviews) if inSample & developer_tag
	egen ss_min_`prefix'Reviews_inSample = min(`prefix'Reviews) if inSample & developer_tag
	egen ss_max_`prefix'Reviews_inSample = max(`prefix'Reviews) if inSample & developer_tag

	drop `prefix'Reviews
}

** Metacritic scores by developer
egen MetacriticScore = mean(metacritic_score), by(developer)

egen ss_avg_MetacriticScore = mean(MetacriticScore) if developer_tag
egen ss_sd_MetacriticScore = sd(MetacriticScore) if developer_tag
egen ss_min_MetacriticScore = min(MetacriticScore) if developer_tag
egen ss_max_MetacriticScore = max(MetacriticScore) if developer_tag

egen ss_avg_MetacriticScore_inSample = mean(MetacriticScore) if inSample & developer_tag
egen ss_sd_MetacriticScore_inSample = sd(MetacriticScore) if inSample & developer_tag
egen ss_min_MetacriticScore_inSample = min(MetacriticScore) if inSample & developer_tag
egen ss_max_MetacriticScore_inSample = max(MetacriticScore) if inSample & developer_tag

** Total estimated sales by developer
egen estimatedSales = total((estimated_owners_lb + estimated_owners_ub) / (2 * 1e6)), by(developer)
replace estimatedSales = . if estimatedSales == 0

egen ss_avg_EstSales = mean(estimatedSales) if developer_tag
egen ss_sd_EstSales  = sd(estimatedSales) if developer_tag
egen ss_min_EstSales = min(estimatedSales) if developer_tag
egen ss_max_EstSales = max(estimatedSales) if developer_tag

egen ss_avg_EstSales_inSample = mean(estimatedSales) if inSample & developer_tag
egen ss_sd_EstSales_inSample  = sd(estimatedSales) if inSample & developer_tag
egen ss_min_EstSales_inSample = min(estimatedSales) if inSample & developer_tag
egen ss_max_EstSales_inSample = max(estimatedSales) if inSample & developer_tag

drop estimatedSales

*******************************************************************************
** Prepare table as dataset	
*******************************************************************************

** Rename variables and keep first row
collapse (mean) ss_* (sum) totalDevelopers=developer_tag totalAcquired=inSample
rename ss_* *

** Reshape to table form
reshape long avg_@ sd_@ min_@ max_@, i(totalDevelopers totalAcquired) j(statistic) string
rename *_ *

** Generate panels
gen panel = (strpos(statistic, "inSample") > 0)
replace statistic = subinstr(statistic, "_inSample", "", .)

label define panelNames 0 "Full sample" 1 "Acquired", replace
label values panel panelNames

order panel, first

** Parse statistics
replace statistic = "Number of Games" 				if strpos(statistic,"Games")
replace statistic = "Estimated Sales (millions)" 	if strpos(statistic,"EstSales")
replace statistic = "Metacritic Score (\%)" 		if strpos(statistic,"Metacritic")
replace statistic = "Positive Reviews (thousands)" 	if strpos(statistic,"PosReviews")
replace statistic = "Negative Reviews (thousands)" 	if strpos(statistic,"NegReviews")

** Set order
gen order = .

replace order = 1 if statistic == "Number of Games"					
replace order = 2 if statistic == "Estimated Sales (millions)"		
replace order = 3 if statistic == "Metacritic Score (\%)"			
replace order = 4 if statistic == "Positive Reviews (thousands)"	
replace order = 5 if statistic == "Negative Reviews (thousands)"	

sort panel order

*******************************************************************************
** Write table
*******************************************************************************

** Open file
file open dev_summary_statistics using "tables/summary statistics/developer_summary_statistics.tex", write all replace 

** Write header
file write dev_summary_statistics "\begin{table}" _n
file write dev_summary_statistics "	\small" _n
file write dev_summary_statistics "	\centering" _n
file write dev_summary_statistics "	\begin{tabular}{lcccc}" _n
file write dev_summary_statistics "		\hline\hline" _n
file write dev_summary_statistics "		\\[-1.5ex]" _n
file write dev_summary_statistics "			&	Mean	&	SD &	Min	&	Max	\\[0.5ex] \hline" _n
file write dev_summary_statistics "		\\[-1.5ex]" _n

** Fill table
forvalues i=1/`=_N' {
	if order[`i'] == 1 {
		if panel[`i'] == 0 {
			local N = strofreal(totalDevelopers, "%9.0fc")

			file write dev_summary_statistics "		\multicolumn{5}{l}{\textbf{Panel (a): Full sample (N = `N')}}				\\[0.5ex]" _n 
		}
		else {
			local N = strofreal(totalAcquired, "%9.0fc")

			file write dev_summary_statistics "		\multicolumn{5}{l}{\textbf{Panel (b): Acquired (N = `N')}}				\\[0.5ex]" _n 
		}
	}

	local newLine = "\quad " + statistic[`i'] + "	&	" 							///
						+ strofreal(avg[`i'], "%9.2f") + "	&	" 		///
						+ strofreal(sd[`i'], "%9.2f")  + "	&	" 		///
						+ strofreal(min[`i'], "%9.2f") + "	&	" 		///
						+ strofreal(max[`i'], "%9.2f") + "	\\[0.5ex]"	//

	file write dev_summary_statistics "`newLine'" _n

	if panel[`=`i'+1'] != panel[`i'] & `i' != `=_N' {
		file write dev_summary_statistics "\\[-1.5ex]" _n
	}

}


** Write footer
file write dev_summary_statistics "		\\[-2ex]" _n
file write dev_summary_statistics "		\hline \hline" _n
file write dev_summary_statistics "	\end{tabular}" _n
file write dev_summary_statistics "\end{table}" _n

** Close file
file close dev_summary_statistics
