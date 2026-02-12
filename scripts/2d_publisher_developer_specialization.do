/*******************************************************************************
Vertical integration and product variety - 
Created: November, 2025
Author: Felipe Del Canto
*******************************************************************************/

/*
	
	
	
*/

*******************************************************************************
** Create folders for figures
*******************************************************************************

!mkdir "figures/genre distribution"

*******************************************************************************
** Prepare developer data
*******************************************************************************

** Open data
use "data/processed_developer_data.dta", clear

** Count number of games per developer and number of games in each genre
egen game_tag = tag(developer name)
egen developer_genre_tag = tag(developer genre)

collapse (sum) num_games=game_tag num_genres=developer_genre_tag (first) inSample, by(developer)

** Winsorize genres
replace num_genres = 15 if num_genres > 15

** Change data format
rename developer name
gen developer = 1

** Label
label var name "Name of firm"
label var num_games "Number of games"
label var num_genres "Number of genres"
label var developer "Developer = 1; Publisher = 0"
label var inSample "In sample (1 = Yes, 0 = No)"

** Save temporary file
tempfile developer_genre_count
save `developer_genre_count', replace

*******************************************************************************
** Prepare publisher data
*******************************************************************************

** Open data
use "data/processed_publisher_data.dta", clear

** Count number of games per publisher and number of games in each genre
egen game_tag = tag(name publisher)
egen publisher_genre_tag = tag(publisher genre)

collapse (sum) num_games=game_tag num_genres=publisher_genre_tag (first) inSample=acquirer, by(publisher)

** Winsorize genres
replace num_genres = 15 if num_genres > 15

** Change data format
rename publisher name
gen developer = 0

** Label
label var name "Name of firm"
label var num_games "Number of games"
label var num_genres "Number of genres"
label var developer "Developer = 1; Publisher = 0"
label var inSample "In sample (1 = Yes, 0 = No)"

** Append developer data
append using `developer_genre_count'

*******************************************************************************
** Create histogram (all units)
*******************************************************************************

** Color
colorpalette reds, select(6) locals(red)
colorpalette reds, select(6) locals(red30) opacity(30)
colorpalette blues, select(7) locals(blue)
colorpalette blues, select(7) locals(blue30) opacity(30)

* Overlapping histograms of num_genres by developer (1) vs publisher (0)
twoway	(histogram num_genres if developer==1, discrete percent lwidth(1pt) lcolor(`blue') fcolor(`blue30')) 	///
		(histogram num_genres if developer==0, discrete percent lwidth(1pt) lcolor(`red') fcolor(`red30')) 		///
		if num_games >= 3, 																						///
			xlabel(0 "0" 3 "3" 6 "6" 9 "9" 12 "12" 15 "15+", labsize(16pt)) 									///
			ylabel(, angle(h) labsize(16pt)) 																	///
			ytitle("Percent (%)", margin(r=1) size(16pt))														///
			xtitle("Number of genres", margin(t=1) size(16pt)) 													///
			legend(order(1 "Developers" 2 "Publishers") size(14pt))						 						///
			name(hist_dev_pub, replace)

graph export "figures/genre distribution/developer-publisher-genre-distribution-histogram.pdf", replace
graph close

*******************************************************************************
** Create histogram (in sample units)
*******************************************************************************

** Color
colorpalette reds, select(6) locals(red)
colorpalette reds, select(6) locals(red30) opacity(30)
colorpalette blues, select(7) locals(blue)
colorpalette blues, select(7) locals(blue30) opacity(30)

* Overlapping histograms of num_genres by developer (1) vs publisher (0)
twoway	(histogram num_genres if developer==1, discrete percent lwidth(1pt) lcolor(`blue') fcolor(`blue30')) 	///
		(histogram num_genres if developer==0, discrete percent lwidth(1pt) lcolor(`red') fcolor(`red30')) 		///
		if inSample, 																							///
			xlabel(0 "0" 3 "3" 6 "6" 9 "9" 12 "12" 15 "15+", labsize(16pt)) 									///
			ylabel(, angle(h) labsize(16pt)) 																	///
			ytitle("Percent (%)", margin(r=1) size(16pt))														///
			xtitle("Number of genres", margin(t=1) size(16pt)) 													///
			legend(order(1 "Developers" 2 "Publishers") size(14pt))						 						///
			name(hist_dev_pub, replace)

graph export "figures/genre distribution/developer-publisher-genre-distribution-histogram-inSample.pdf", replace
graph close

*******************************************************************************
** Kolmogorov-Smirnov test for full sample
*******************************************************************************

** Drop weird observation with missing name (TODO)
drop if num_games <= 3

** Regress number of genres on developer indicator
reg num_genres developer, r

** Count total number of observations in developer/publisher
egen totalObs = count(num_games), by(developer)

** Count number of observations within each genre count
collapse (count) numObs=num_games (first) totalObs, by(developer num_genres)
gen percObs = numObs / totalObs

ksmirnov percObs, by(developer) exact