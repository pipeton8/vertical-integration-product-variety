/*******************************************************************************
Vertical integration and product variety - Process publishers
Created: August, 2025
Author: Felipe Del Canto
*******************************************************************************/

/*
	
	
	
*/

*******************************************************************************
** Import developer data and run initial processing
*******************************************************************************

** Import
import delimited "data/publisher_game_data.csv", clear
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
** Consolidate names of publishers and handle duplicates
*******************************************************************************

** Consolidate names of publishers
replace publisher = "Atari SA" 									if inlist(publisher, "Atari")		
replace publisher = "2K" 										if inlist(publisher, "2K Games")
replace publisher = "SEGA" 										if inlist(publisher, "SEGA (Japan)")
replace publisher = "Zynga" 									if inlist(publisher, "Zynga Inc.")
replace publisher = "Tencent"									if inlist(publisher, "Tencent Games")
replace publisher = "THQ Nordic"								if inlist(publisher, "THQ")
replace publisher = "Sony Interactive Entertainment" 			if inlist(publisher, 	"Sony", 								 			///
																						"Sony Corporation",									///
																						"Sony Music Entertainment",							///
																						"Sony Music Entertainment (Japan) Inc. / UNTIES",	///
																						"Sony Pictures Virtual Reality", 					///
																						"Sony Pictures Virtual Reality (SPVR)"				///
																		)																	//
replace publisher = "Microsoft"									if inlist(publisher, "Microsoft Corporation", "Microsoft Studios")
replace publisher = "Warner Bros. Interactive Entertainment"	if inlist(publisher, "Warner Bros. Games", "Warner Bros. Interactive")

** Handle duplicated games
collapse (first) 	app_id release_date price																				///
		 (max) 		positive_reviews negative_reviews metacritic_score peak_ccu estimated_owners_lb estimated_owners_ub,	///
	by(name publisher genre)

*******************************************************************************
** Merge with acquisition data
*******************************************************************************

preserve

	** Import data
	import delimited "data/raw/acquisitions.csv", clear
	keep publisher developer acquisition_date
	sort publisher acquisition_date

	** For publisher that acquire multiple times, need wide format
	bysort publisher: gen j = _n
	qui sum j
	local max_j = r(max)

	** Reshape to wide format
  	reshape wide acquisition_date developer, i(publisher) j(j)

	** Save
	tempfile acquisitions
	save `acquisitions', replace
restore

** Merge
merge m:1 publisher using `acquisitions'
drop _merge

*******************************************************************************
** Create relevant variables
*******************************************************************************

** Acquirer dummy
gen acquirer = !missing(acquisition_date1)

** Generate year of release
gen year = year(release_date)
order year, after(release_date)

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
sort publisher release_date name

** Label variables
label var name 					"Game name"
label var publisher 			"Publisher"
label var genre 				"Genre"
label var app_id 				"Steam ID"
label var release_date 			"Release date"
label var year 					"Release year"
label var price 				"Price"
label var positive_reviews 		"Positive reviews"
label var negative_reviews 		"Negative reviews"
label var positive_share 		"Share of positive reviews"
label var total_reviews 		"Total reviews"
label var estimated_sales 		"Estimated sales (thousands)"
label var metacritic_score 		"Metacritic score"
label var peak_ccu 				"Peak concurrent users"
label var estimated_owners_lb 	"Estimated owners (lower bound)"
label var estimated_owners_ub 	"Estimated owners (upper bound)"

forvalues i=1/`max_j' {
	label var acquisition_date`i' "Acquisition date `i'"
	label var developer`i' "Acquired developer `i'"
}

label var acquirer 				"Acquirer (1 = Yes, 0 = No)"

** Save
compress
save "data/processed_publisher_data.dta", replace