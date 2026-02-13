/*******************************************************************************
Vertical integration and product variety - Compute specializations
Created: November, 2025
Author: Felipe Del Canto
*******************************************************************************/

/*
	
	
	
*/

*******************************************************************************
** Compute portfolio vector of acquired developers before acquisition
*******************************************************************************

** Open data
use "data/processed_developer_data.dta", clear

** Keep only the games before acquisiton
keep if !acquired & inSample

** Count number of games per developer
egen game_tag = tag(name)
egen num_games_dev = total(game_tag), by(developer)

** Collapse at the developer-genre level
collapse (count) num_games_genre=app_id (first) num_games_dev acquisition_date (mean) metacritic_score estimated_sales positive_share peak_ccu, by(developer genre)
gen developer_genre_share = num_games_genre / num_games_dev * 100
drop num_games_genre

** Add acquirer publisher to handle missing genres from publisher
preserve
	import delimited "data/raw/acquisitions.csv", clear

	keep developer publisher acquisition_date

	tempfile acquisitions
	save `acquisitions', replace
restore

merge m:1 developer acquisition_date using `acquisitions'
keep if _merge == 3
drop _merge

** Save for later
tempfile developer_portfolios
save `developer_portfolios', replace

*******************************************************************************
** Prepare developer data
*******************************************************************************

** Open data
use "data/processed_publisher_data.dta", clear

** Generate year of release
gen release_year = year(release_date)

*******************************************************************************
** Check crowdedness of genres 
*******************************************************************************

** Compute crowdedness
preserve 
	collapse (count) games_in_genre=app_id, by(genre release_year)

	replace games_in_genre = games_in_genre / 1000

	tempfile genre_crowdedness
	save `genre_crowdedness', replace
restore

*******************************************************************************
** Compute publisher portfolios before each acquisition
*******************************************************************************

** Keep only acquiring publishers
keep if acquirer

** Keep only relevant variables
keep name publisher genre app_id release_year acquisition_date* developer*

** Reshape long
reshape long acquisition_date developer, i(name publisher app_id genre release_year) j(acq_num)
drop acq_num

** Keep games only if they were launched before the acquisition date
keep if release_year <= acquisition_date & !missing(acquisition_date)

** Compute portfolio vector
egen game_tag = tag(name acquisition_date developer)
egen num_games = total(game_tag), by(publisher acquisition_date developer)

collapse (count) num_games_genre=app_id (first) num_games, by(publisher acquisition_date developer genre)
gen publisher_genre_share = num_games_genre / num_games * 100 
drop num_games

rename num_games_genre publisher_num_games_genre

*******************************************************************************
** Merge developer and publisher portfolios
*******************************************************************************

** Merge
merge 1:1 genre developer acquisition_date using `developer_portfolios'
drop _merge

** Create acquisition tag
egen acquisition_tag = tag(publisher acquisition_date developer)

** For those publisher-genre or developer-genre combinations that are not
** in both datasets shares are equal to 0
foreach g in publisher developer {
	replace `g'_genre_share = 0 if missing(`g'_genre_share)
}

replace publisher_num_games_genre = 0 if missing(publisher_num_games_genre)

*******************************************************************************
** Compute similarity
*******************************************************************************

** Compute cosine similarity elements
egen raw_genre_similarity = total(publisher_genre_share * developer_genre_share), by(publisher acquisition_date developer)
egen developer_genre_norm = total(developer_genre_share^2), by(developer)
egen publisher_genre_norm = total(publisher_genre_share^2), by(publisher acquisition_date developer)

** Compute cosine similarity
gen genre_similarity = raw_genre_similarity / (sqrt(publisher_genre_norm) * sqrt(developer_genre_norm))
drop developer_genre_norm publisher_genre_norm raw_genre_similarity

** Drop cases with missing similarity
drop if missing(genre_similarity)

** Classify as specialization or diversification
gen specialization_hc = 0
replace specialization_hc = 1 if genre_similarity > 0.5

sum genre_similarity if acquisition_tag, d
gen genre_similarity_median = `r(p50)'
gen specialization_median = genre_similarity >= genre_similarity_median

*******************************************************************************
** Incorporate crowdedness
*******************************************************************************

** Merge crowdedness
rename acquisition_date release_year
merge m:1 genre release_year using `genre_crowdedness'
keep if _merge == 3
drop _merge
rename release_year acquisition_date

*******************************************************************************
** Run analysis
*******************************************************************************

** Label variables
label var acquisition_date 			"Acquisition year"
label var developer 				"Developer"
label var publisher 				"Publisher"
label var publisher_num_games_genre "Number of games in genre by publisher pre-acquisition"
label var publisher_genre_share 	"Share of games in genre by publisher pre-acquisition"
label var num_games_dev 			"Number of games by developer until year"
label var developer_genre_share 	"Share of games in genre by developer pre-acquisition"
label var acquisition_tag 			"Acquisition identifier"
label var genre_similarity 			"Genre similarity between publisher and developer pre-acquisition"
label var specialization_hc 		"Specialization indicator (high cosine similarity > 0.5)"
label var specialization_median 	"Specialization indicator (cosine similarity > median)"
label var games_in_genre 			"Genre crowdedness (thousands of games released in genre up to that year)"

** Close dataset
compress
save "data/acquisitions_with_specialization.dta", replace