/*******************************************************************************
Vertical integration and product variety - Determinants of specializations
Created: November, 2025
Author: Felipe Del Canto
*******************************************************************************/

/*
	
	
	
*/

*******************************************************************************
** Create folder for figures
*******************************************************************************

!mkdir "figures/developer distribution"

*******************************************************************************
** Prepare data
*******************************************************************************

** Open data
use "data/processed_developer_data.dta", clear

** Rename sample indicator to "acquired"
drop acquired
rename inSample acquired

** Count number of games per developer
egen game_tag = tag(name)
egen num_games_dev = total(game_tag), by(developer)

** Collapse at the developer-genre level
collapse (count) num_games_genre=app_id (first) num_games_dev acquired, by(developer genre)
gen developer_genre_share = num_games_genre / num_games_dev * 100
drop num_games_genre num_games_dev

** Encode genre
rename genre genre_str
encode genre_str, gen(genre)
order genre, after(genre_str)
drop genre_str

** Reshape
reshape wide developer_genre_share, i(developer acquired) j(genre)

** Replace missings with zeros
foreach var of varlist developer_genre_share* {
	replace `var' = 0 if missing(`var')
}

*******************************************************************************
** Create tSNE data with python
*******************************************************************************

tic
python:

from sfi import Data
import numpy as np
from sklearn.manifold import TSNE

# Load data from Stata
X = np.array(Data.get())[:, 2:]  # Skip developer and acquired columns
X = np.array(X, dtype=np.float64)

# Apply t-SNE
tsne = TSNE(n_components=2, random_state=42)
X_tsne = tsne.fit_transform(X)

# Save results back to Stata
Data.addVarDouble('tsne_x1')
Data.addVarDouble('tsne_x2')

Data.store(['tsne_x1', 'tsne_x2'], None, X_tsne)
end
toc

*******************************************************************************
** Create tSNE graph
*******************************************************************************

** Take a sample of non-acquired firms
set seed 12345
gen u = runiform() if acquired == 0
gen sample_tag = u < 0.1
gen low_opacity_tag = u < 0.05 if ((tsne_x1 + 25)^2 + tsne_x2^2 < 1000)
drop u

** Colors
colorpalette Blues, select(6) locals(blue_full)
colorpalette Blues, select(6) opacity(30) locals(blue_50)
colorpalette Blues, select(6) opacity(20) locals(blue_20)
colorpalette Reds, select(7) locals(red)

** Create graph
twoway	(scatter tsne_x2 tsne_x1 if acquired < 0, msymbol(O) mcolor(`blue_full') mlcolor(%0) msize(5pt)) 	///
		(scatter tsne_x2 tsne_x1 if acquired == 0 & sample_tag & missing(low_opacity_tag), msymbol(O) mcolor(`blue_50') mlcolor(%0) msize(5pt)) 	///
		(scatter tsne_x2 tsne_x1 if acquired == 0 & low_opacity_tag == 1, msymbol(O) mcolor(`blue_20') mlcolor(%0) msize(5pt)) 					///
		(scatter tsne_x2 tsne_x1 if acquired == 1, msymbol(O) mcolor(`red') msize(7pt)), 												///
	legend(order(1 "Independent Developers" 4 "Acquired Developers") size(14pt)) 														///
	xlabel(none)																															///
	ylabel(none) 																															///
	xtitle("tSNE Dimension 1", margin(t=2) size(16pt)) 																					///
	ytitle("tSNE Dimension 2", margin(r=1) size(16pt)) 																					//

** Save graph
graph export "figures/developer distribution/developer-tsne-distribution.pdf", replace
graph close 

