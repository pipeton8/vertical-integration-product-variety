/*******************************************************************************
Vertical integration and product variety - Process developers
Created: August, 2025
Author: Felipe Del Canto
*******************************************************************************/

/*
	
	
	
*/

*******************************************************************************
** Import developer data and run initial processing
*******************************************************************************

** Import
import delimited "data/developer_game_data.csv", clear
compress

** Parse estimated owners
split estimated_owners, parse("-")
order estimated_owners?, after(estimated_owners)
destring estimated_owners?, replace
rename (estimated_owners1 estimated_owners2) (estimated_owners_lb estimated_owners_ub)
drop estimated_owners

** Format date. Originally in "M D, Y" format
rename release_date release_date_str
gen release_date = date(release_date_str, "MDY")
format release_date %td
drop release_date_str

*******************************************************************************
** Consolidate names of developers and handle duplicates
*******************************************************************************

** Rovio
replace developer = "Rovio Entertainment" 		if inlist(developer, "Rovio Sweden AB")
replace developer = "Zynga" 					if inlist(developer, "Zynga Inc.")
replace developer = "Daedalic Entertainment" 	if inlist(developer, "Daedalic Studio West")
replace developer = "DotEmu" 					if inlist(developer, "DotEmu SAS.", "Dotemu")
replace developer = "Croteam" 					if inlist(developer, "Croteam Incubator", "Croteam VR")
replace developer = "CCP Games" 				if inlist(developer, "CCP")
replace developer = "Respawn Entertainment" 	if inlist(developer, "Respawn")
replace developer = "PopCap Games" 				if inlist(developer, "PopCap", "PopCap Games, Inc.")
replace developer = "Eidos Interactive" 		if inlist(developer, "Eidos Montreal", "Eidos-Montréal", "Eidos Studio Hungary")
replace developer = "BioWare" 					if inlist(developer, "BioWare Corporation")
replace developer = "TT Games" 					if inlist(developer, "TT Fusion", "TT Games Ltd")

** Handle duplicated games
collapse (first) 	app_id release_date price																				///
		 (max) 		positive_reviews negative_reviews metacritic_score peak_ccu estimated_owners_lb estimated_owners_ub,	///
	by(name developer genre)

*******************************************************************************
** Merge with acquisition data
*******************************************************************************

preserve

	** Import data
	import delimited "data/raw/acquisitions.csv", clear
	keep developer acquisition_date

	** Save
	tempfile acquisitions
	save `acquisitions', replace
restore

** Merge
merge m:1 developer using `acquisitions'
// keep if _merge==3
drop _merge

*******************************************************************************
** Create relevant variables
*******************************************************************************

** Sample indicator
gen inSample = !missing(acquisition_date)

** Acquisition indicator
gen year = year(release_date)
order year, after(release_date)
gen acquired = year > acquisition_date if !missing(acquisition_date)

** Quality metrics
gen total_reviews = positive_reviews + negative_reviews
gen positive_share = positive_reviews / total_reviews
order total_reviews positive_share, after(negative_reviews)

** Code missing metacritic score
replace metacritic_score = . if metacritic_score == 0

** Estimated sales
gen estimated_sales = (estimated_owners_lb + estimated_owners_ub) / (2 * 1e3)
replace estimated_sales = . if estimated_sales == 0

*******************************************************************************
** Sample selection
*******************************************************************************

** Keep games since 2007
keep if year >= 2007

*******************************************************************************
** Save
*******************************************************************************

** Sort
sort developer release_date name

** Label variables
label var name 					"Game name"
label var developer 			"Developer"
label var genre 				"Genre"
label var app_id 				"Steam ID"
label var release_date 			"Release date"
label var year 					"Release year"
label var price 				"Price"
label var positive_reviews 		"Positive reviews"
label var negative_reviews 		"Negative reviews"
label var total_reviews 		"Total reviews"
label var positive_share 		"Share of positive reviews"
label var metacritic_score 		"Metacritic score"
label var estimated_sales 		"Estimated sales (thousands)"
label var peak_ccu 				"Peak concurrent users"
label var estimated_owners_lb 	"Estimated owners (lower bound)"
label var estimated_owners_ub 	"Estimated owners (upper bound)"
label var acquisition_date 		"Acquisition date"
label var acquired 				"Acquired (1 = Yes, 0 = No)"
label var inSample 				"In sample (1 = Yes, 0 = No)"

** Save
compress
save "data/processed_developer_data.dta", replace