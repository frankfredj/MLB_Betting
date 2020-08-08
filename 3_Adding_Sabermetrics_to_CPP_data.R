################################################
################ INITIALISATION ################
################################################

n = 25
path = "D:/MLB"

library("FNN")
library("missRanger")
library("ggplot2")
library("e1071")
library("caret")
library("glmnet")
library("viridis")
library("pracma")
library("reshape2")
library("doParallel")
library("cowplot")
library("stringr")

################################################
################################################


################################################
################ Functions #####################
################################################

#Given an index (a1 ... ak), produces an index with (a1_home, a1_away, ... , ak_home, ak_away)
symetrical_index <- function(index, X){

	m <- ncol(X) / 2

	u <- which(index > m)
	if(length(u) > 0){

		a <- index[u] - m
		b <- index[-u] + m		

	} else {

		a <- index
		b <- index + m	

	}

	return(sort(unique(c(a,b,index))))

}



#Plot the stacked ecdf and epdf functions of a vector
density_plots <- function(x, varname, title){

	unique_vals <- unique(sort(x))
	pdf <- ecdf(x)

	plot_frame <- as.data.frame(matrix(nrow = length(unique_vals), ncol = 3))
	colnames(plot_frame) <- c("X", "Density", "Ecdf")
	plot_frame$X <- unique_vals

	f <- approxfun(density(x))
	plot_frame$Epdf <- f(plot_frame$X)

	f <- ecdf(x)
	plot_frame$Ecdf <- f(plot_frame$X)

	plot_frame$Ecdf <- plot_frame$Ecdf / max(plot_frame$Ecdf)
	plot_frame$Epdf <- plot_frame$Epdf / max(plot_frame$Epdf)

	k <- nrow(plot_frame)

	melted_plot <- as.data.frame(matrix(nrow = 2*k, ncol = 3))
	colnames(melted_plot) <- c("X", "Y", "Legend")
	melted_plot$Y <- c(plot_frame$Epdf, plot_frame$Ecdf)
	melted_plot$X <- c(plot_frame$X, plot_frame$X)
	melted_plot$Legend <- c(rep("Epdf", k), rep("Ecdf", k))

	out <- ggplot(melted_plot, aes(x = X, y = Y, fill = Legend)) + geom_step(aes(x = X, y = Y, color = Legend), alpha = 2, size = 1.5) +
															geom_ribbon(aes(ymin = 0, ymax = Y, fill = Legend), alpha = 0.5) +
															xlab(varname) +
															ylab("Value") +
															ggtitle(title) + 
															theme(axis.title=element_text(size=14, face="bold"),
																plot.title = element_text(size=18, face="bold"))

	return(out)		

}





#Dummy function for knn outliers
knn_outliers_dummy <- function(x, nn, n_sd){

	dists <- rep(0, length(x))
	index <- order(x)
	x_sort <- x[index]

	for(i in 1:length(x)){

		index_temp <- c((i - nn - 1):(i + nn + 1))
		index_temp <- index_temp[which(index_temp >= 1 & index_temp <= length(x))]

		eucl_dist <- sort(abs(x_sort[i] - x_sort[index_temp]))[c(2:(nn+1))]
		dists[index[i]] <- mean(eucl_dist)

	}

	dists <- dists - mean(dists) / sd(dists)

	return(dists)

}



# Trim a numerical vector by recursively removing outliers until the normalized 
# distribution of knn distances given nn is comprised within [-n_sd, n_sd]
knn_outliers <- function(X, nn, n_sd, j){

	x <- as.numeric(X[, j])
	rmv <- c()
	index <- c(1:length(x))
	avg_dists <- knn_outliers_dummy(x, nn, n_sd)

	k <- 0

	old_range <- range(x)
	old_skew <- skewness(x)

	original_data <- x

	while(TRUE){
	#for(i in 1:134){
		k <- k + 1

		to_flush <- which(abs(avg_dists) >= n_sd)
		if(length(to_flush) == 0){break}

		rmv <- c(rmv, index[to_flush])

		x <- x[-to_flush]
		index <- index[-to_flush]	


		avg_dists <- knn_outliers_dummy(x, nn, n_sd)

	}

	if(length(rmv) > 0){

		print("Old range:", quote = FALSE)
		print(old_range)
		print("Old skewness:", quote = FALSE)
		print(old_skew)

		cat("\n")

		print("New range:", quote = FALSE)
		print(range(x))
		print("New skewness:", quote = FALSE)
		print(skewness(x))

		plot_a <- density_plots(original_data, colnames(X)[j], "Original Data")
		plot_b <- density_plots(x, colnames(X)[j], "Clean Data")

		print(cowplot::plot_grid(plotlist = list(plot_a, plot_b), nrow = 1))


	} 

	return(rmv)

}


#Prints a correlation heatmap of selected variables
cor_heatmap <- function(frame, type, variables, title, names){

	index <- match(variables, colnames(frame))
	if(type == "cor"){

		cors <- cor(frame[, index])

	} else {

		cors <- cov(frame[, index])

	}

	plot_frame <- cbind(expand.grid(X = rownames(cors), Y = colnames(cors)), expand.grid(cors))
	colnames(plot_frame)[3] <- "Value"


	m <- ncol(cors)
	if(names){

		plt <- 	ggplot(plot_frame, aes(X, Y, fill= Value)) + geom_tile() +
  												scale_fill_viridis(discrete=FALSE)+ 
  												theme(axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1)) +
  												labs(title = title) 

	} else {


		plt <- 	ggplot(plot_frame, aes(X, Y, fill= Value)) + geom_tile() +
  												scale_fill_viridis(discrete=FALSE) +
												  theme(axis.title.x=element_blank(),
												        axis.text.x=element_blank(),
												        axis.ticks.x=element_blank(),
												        axis.title.y=element_blank(),
												        axis.text.y=element_blank(),
												        axis.ticks.y=element_blank()) +
												  labs(title = title)
	}


	return(plt)

}


#Function to add bat sabermetrics
bat_sabermetrics <- function(bat, location){

	bat$BIP <- bat$AB - bat$SO - bat$HR + bat$SF
	bat$ISO <- (bat$X2B + 2*bat$X3B + 3*bat$HR) / bat$AB	
	bat$BABIP <- (bat$H - bat$HR) / (bat$AB - bat$SO - bat$HR + bat$SF)
	
	bat$OBP <- (bat$H + bat$BB + bat$HBP) / (bat$AB + bat$BB + bat$HBP + bat$SF)
	bat$SLG <- (bat$X1B + 2*bat$X2B + 3*bat$X3B + 4*bat$HR) / bat$AB

	bat$BB_SO_ratio <- bat$BB / bat$SO
	bat$OPS <- bat$OBP + bat$SLG

	bat$GB_FB_ratio <- bat$GB / bat$FB

	bat$LD_percent <- bat$LD / bat$BIP
	bat$FB_percent <- bat$FB / bat$BIP
	bat$GB_percent <- bat$GB / bat$BIP
	bat$IFFB_percent <- bat$IFFB / bat$FB

	bat$HR_FB_ratio <- bat$HR / bat$FB
	bat$IFH_percent <- bat$IFH / bat$GB

	bat$WPA_pLI_ratio <- bat$WPA / bat$pLI
	bat$WPA_LI_ratio <- bat$WPA / bat$LI
	bat$Clutch <- bat$WPA_LI_ratio - bat$WPA_pLI_ratio

	colnames(bat) <- paste("Bat", colnames(bat), location, sep = "_")
	return(bat)

}



pitch_sabermetrics <- function(pitch, location){

		#fip_constant = (frame$FIP - (13*frame$HR + 3*(frame$BB + frame$HBP) - 2*frame$SO) / frame$IP).mean()
		#pitch$FIP = (13*pitch$HR + 3*(pitch$BB + pitch$HBP) - 2*pitch$SO) / pitch$IP + fip_constant
		pitch$FIP = (13*pitch$HR + 3*(pitch$BB + pitch$HBP) - 2*pitch$SO) 

		pitch$SO_BB_ratio = pitch$SO / pitch$BB

		pitch$HR_FB_ratio = pitch$HR / pitch$FB

		pitch$BABIP = (pitch$H - pitch$HR) / pitch$BIP

		pitch$LOB_percent = (pitch$H + pitch$BB + pitch$HBP - pitch$R) / (pitch$H + pitch$BB + pitch$HBP - 1.4*pitch$HR)
		
		pitch$LD_percent = pitch$LD / pitch$BIP
		pitch$FB_percent = pitch$FB / pitch$BIP
		pitch$GB_percent = pitch$GB / pitch$BIP
		
		pitch$BB_percent = pitch$BB / pitch$TBF
		pitch$K_percent = pitch$SO / pitch$TBF
		pitch$K_minus_BB_percent = pitch$K_percent - pitch$BB_percent

		pitch$IFFB_percent = pitch$IFFB / pitch$FB
		pitch$IFH_percent = pitch$IFH / pitch$GB
		pitch$BUH_percent = pitch$BUH / pitch$BU


		pitch$WPA_LI_ratio = pitch$WPA / pitch$LI
		pitch$WPA_pLI_ratio = pitch$WPA / pitch$pLI
		pitch$Clutch = pitch$WPA_LI_ratio - pitch$WPA_pLI_ratio	
	
		colnames(pitch) <- paste("Pitch", colnames(pitch), location, sep = "_")	
		return(pitch)	

}


################################################
################################################



#####################################################
################ Data retrieval #####################
#####################################################

#Retrieve frames, add Y binary A_Win and B_Loss variables
X <- readRDS(paste(path, "/MLB_Modeling/Regression/", n, "/R_regression_matrix_clean.rds", sep = ""))
data <- readRDS(paste(path, "/MLB_Modeling/Regression/", n, "/R_regression_data_clean.rds", sep = ""))


data$Win <- 1
data$Win[which(data$Score_Home < data$Score_Away)] <- 0

Y <- as.matrix(data$Win)
Y_factor <- rep("A_Win", length(Y))
Y_factor[which(Y == 0)] <- "B_Loss"
Y_factor <- as.factor(Y_factor)


#####################################################
#####################################################


#######################################################
################ Add Sabermetrics #####################
#######################################################

#Split the data
X_split <- list()
X_split$bat_home <- X[, c(1:40)]
X_split$pitch_home <- X[, c(41:80)]
X_split$bat_away <- X[, c(81:120)]
X_split$pitch_away <- X[, c(121:160)]


#Remove column names identifiers
for(i in 1:length(X_split)){

	new_colnames <- rep("", ncol(X_split[[i]]))
	splitted_values <- str_split(colnames(X_split[[i]]), "_", simplify = TRUE)

	for(j in 1:nrow(splitted_values)){

		if(splitted_values[j, ncol(splitted_values)] == ""){

			new_colnames[j] <- splitted_values[j, 2]

		} else {

			new_colnames[j] <- paste(splitted_values[j, 2], splitted_values[j, 3], sep = "")

		}

	}

	colnames(X_split[[i]]) <- new_colnames

}


X_split[[1]] <- bat_sabermetrics(X_split[[1]], "Home")
X_split[[3]] <- bat_sabermetrics(X_split[[3]], "Away")

X_split[[2]] <- pitch_sabermetrics(X_split[[2]], "Home")
X_split[[4]] <- pitch_sabermetrics(X_split[[4]], "Away")


X <- cbind(X_split[[1]], X_split[[2]], X_split[[3]], X_split[[4]])
saveRDS(X, paste(path, "/MLB_Modeling/Regression/", n, "/R_regression_sabermetrics.rds", sep = ""))



