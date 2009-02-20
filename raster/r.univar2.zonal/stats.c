/*
 *  Calculates univariate statistics from the non-null cells
 *
 *   Copyright (C) 2004-2007 by the GRASS Development Team
 *   Author(s): Hamish Bowman, University of Otago, New Zealand
 *              Martin Landa and Soeren Gebbert
 *
 *      This program is free software under the GNU General Public
 *      License (>=v2). Read the file COPYING that comes with GRASS
 *      for details.
 *
 */

#include "globals.h"

/* *************************************************************** */
/* **** univar_stat constructor ********************************** */
/* *************************************************************** */
univar_stat *create_univar_stat_struct(int map_type, int size, int n_perc)
{
    univar_stat *stats;
    int i;
    int n_zones = zone_info.n_zones;

    stats = (univar_stat *) G_calloc(n_zones, sizeof(univar_stat));

    for (i = 0; i < n_zones; i++) {
	stats[i].sum = 0.0;
	stats[i].sumsq = 0.0;
	stats[i].min = 0.0 / 0.0;	/*set to nan as default */
	stats[i].max = 0.0 / 0.0;	/*set to nan as default */
	stats[i].n_perc = n_perc;
	if (n_perc > 0)
	    stats[i].perc = (int *)G_malloc(n_perc * sizeof(int));
	else
	    stats[i].perc = NULL;
	stats[i].sum_abs = 0.0;
	stats[i].n = 0;
	stats[i].size = size;
	stats[i].dcell_array = NULL;
	stats[i].fcell_array = NULL;
	stats[i].cell_array = NULL;
	stats[i].map_type = map_type;

	/* allocate memory for extended computation */
	if (param.extended->answer) {
	    if (map_type == DCELL_TYPE)
		stats[i].dcell_array =
		    (DCELL *) G_calloc(stats[i].size, sizeof(DCELL));
	    if (map_type == FCELL_TYPE)
		stats[i].fcell_array =
		    (FCELL *) G_calloc(stats[i].size, sizeof(FCELL));
	    if (map_type == CELL_TYPE)
		stats[i].cell_array = (CELL *) G_calloc(stats[i].size, sizeof(CELL));
	}
    }

    return stats;
}


/* *************************************************************** */
/* **** univar_stat destructor *********************************** */
/* *************************************************************** */
void free_univar_stat_struct(univar_stat * stats)
{
    int i;

    for (i = 0; i < zone_info.n_zones; i++){
    if (stats[i].perc)
	G_free(stats[i].perc);
    if (stats[i].dcell_array)
	G_free(stats[i].dcell_array);
    if (stats[i].fcell_array)
	G_free(stats[i].fcell_array);
    if (stats[i].cell_array)
	G_free(stats[i].cell_array);
    }

    G_free(stats);

    return;
}


/* *************************************************************** */
/* **** compute and print univar statistics to stdout ************ */
/* *************************************************************** */
int print_stats(univar_stat * stats)
{
    int z;

    for (z = 0; z < zone_info.n_zones; z++) {
	char sum_str[100];
	double mean, variance, stdev, var_coef;

	/*for extendet stats */
	double quartile_25 = 0.0, quartile_75 = 0.0, *quartile_perc;
	double median = 0.0;
	unsigned int i;
	int qpos_25, qpos_75, *qpos_perc;

	/* stats collected for this zone? */
	if (stats[z].n == 0)
	    continue;


	/* all these calculations get promoted to doubles, so any DIV0 becomes nan */
	mean = stats[z].sum / stats[z].n;
	variance = (stats[z].sumsq - stats[z].sum * stats[z].sum / stats[z].n) / stats[z].n;
	if (variance < GRASS_EPSILON)
	    variance = 0.0;
	stdev = sqrt(variance);
	var_coef = (stdev / mean) * 100.;	/* perhaps stdev/fabs(mean) ? */

	sprintf(sum_str, "%.10f", stats[z].sum);
	G_trim_decimal(sum_str);

	fprintf(stdout, "\nzone %d %s\n\n", z + zone_info.min, G_get_c_raster_cat(&z + zone_info.min, &(zone_info.cats)));

	if (!param.shell_style->answer) {
	    fprintf(stdout, "total null and non-null cells: %d\n", stats[z].size);
	    fprintf(stdout, "total null cells: %d\n\n", stats[z].size - stats[z].n);
	    fprintf(stdout, "Of the non-null cells:\n----------------------\n");
	}

	if (param.shell_style->answer) {
	    fprintf(stdout, "n=%d\n", stats[z].n);
	    fprintf(stdout, "null_cells=%d\n", stats[z].size - stats[z].n);
	    fprintf(stdout, "min=%.15g\n", stats[z].min);
	    fprintf(stdout, "max=%.15g\n", stats[z].max);
	    fprintf(stdout, "range=%.15g\n", stats[z].max - stats[z].min);
	    fprintf(stdout, "mean=%.15g\n", mean);
	    fprintf(stdout, "mean_of_abs=%.15g\n", stats[z].sum_abs / stats[z].n);
	    fprintf(stdout, "stddev=%.15g\n", stdev);
	    fprintf(stdout, "variance=%.15g\n", variance);
	    fprintf(stdout, "coeff_var=%.15g\n", var_coef);
	    fprintf(stdout, "sum=%s\n", sum_str);
	}
	else {
	    fprintf(stdout, "n: %d\n", stats[z].n);
	    fprintf(stdout, "minimum: %g\n", stats[z].min);
	    fprintf(stdout, "maximum: %g\n", stats[z].max);
	    fprintf(stdout, "range: %g\n", stats[z].max - stats[z].min);
	    fprintf(stdout, "mean: %g\n", mean);
	    fprintf(stdout, "mean of absolute values: %g\n",
		    stats[z].sum_abs / stats[z].n);
	    fprintf(stdout, "standard deviation: %g\n", stdev);
	    fprintf(stdout, "variance: %g\n", variance);
	    fprintf(stdout, "variation coefficient: %g %%\n", var_coef);
	    fprintf(stdout, "sum: %s\n", sum_str);
	}


	/* TODO: mode, skewness, kurtosis */
	if (param.extended->answer) {
	    qpos_perc = (int *)G_calloc(stats[z].n_perc, sizeof(int));
	    quartile_perc = (double *)G_calloc(stats[z].n_perc, sizeof(double));
	    for (i = 0; i < stats[z].n_perc; i++) {
		qpos_perc[i] = (int)(stats[z].n * 1e-2 * stats[z].perc[i] - 0.5);
	    }
	    qpos_25 = (int)(stats[z].n * 0.25 - 0.5);
	    qpos_75 = (int)(stats[z].n * 0.75 - 0.5);

	    switch (stats[z].map_type) {
	    case CELL_TYPE:
		heapsort_int(stats[z].cell_array, stats[z].n);

		quartile_25 = (double)stats[z].cell_array[qpos_25];
		if (stats[z].n % 2)	/* odd */
		    median = (double)stats[z].cell_array[(int)(stats[z].n / 2)];
		else		/* even */
		    median =
			(double)(stats[z].cell_array[stats[z].n / 2 - 1] +
				 stats[z].cell_array[stats[z].n / 2]) / 2.0;
		quartile_75 = (double)stats[z].cell_array[qpos_75];
		for (i = 0; i < stats[z].n_perc; i++) {
		    quartile_perc[i] = (double)stats[z].cell_array[qpos_perc[i]];
		}
		break;

	    case FCELL_TYPE:
		heapsort_float(stats[z].fcell_array, stats[z].n);

		quartile_25 = (double)stats[z].fcell_array[qpos_25];
		if (stats[z].n % 2)	/* odd */
		    median = (double)stats[z].fcell_array[(int)(stats[z].n / 2)];
		else		/* even */
		    median =
			(double)(stats[z].fcell_array[stats[z].n / 2 - 1] +
				 stats[z].fcell_array[stats[z].n / 2]) / 2.0;
		quartile_75 = (double)stats[z].fcell_array[qpos_75];
		for (i = 0; i < stats[z].n_perc; i++) {
		    quartile_perc[i] = (double)stats[z].fcell_array[qpos_perc[i]];
		}
		break;

	    case DCELL_TYPE:
		heapsort_double(stats[z].dcell_array, stats[z].n);

		quartile_25 = stats[z].dcell_array[qpos_25];
		if (stats[z].n % 2)	/* odd */
		    median = stats[z].dcell_array[(int)(stats[z].n / 2)];
		else		/* even */
		    median =
			(stats[z].dcell_array[stats[z].n / 2 - 1] +
			 stats[z].dcell_array[stats[z].n / 2]) / 2.0;
		quartile_75 = stats[z].dcell_array[qpos_75];
		for (i = 0; i < stats[z].n_perc; i++) {
		    quartile_perc[i] = stats[z].dcell_array[qpos_perc[i]];
		}
		break;

	    default:
		break;
	    }

	    if (param.shell_style->answer) {
		fprintf(stdout, "first_quartile=%g\n", quartile_25);
		fprintf(stdout, "median=%g\n", median);
		fprintf(stdout, "third_quartile=%g\n", quartile_75);
		for (i = 0; i < stats[z].n_perc; i++) {
		    fprintf(stdout, "percentile_%d=%g\n", stats[z].perc[i],
			    quartile_perc[i]);
		}
	    }
	    else {
		fprintf(stdout, "1st quartile: %g\n", quartile_25);
		if (stats[z].n % 2)
		    fprintf(stdout, "median (odd number of cells): %g\n", median);
		else
		    fprintf(stdout, "median (even number of cells): %g\n",
			    median);
		fprintf(stdout, "3rd quartile: %g\n", quartile_75);


		for (i = 0; i < stats[z].n_perc; i++) {
		    if (stats[z].perc[i] % 10 == 1 && stats[z].perc[i] != 11)
			fprintf(stdout, "%dst percentile: %g\n", stats[z].perc[i],
				quartile_perc[i]);
		    else if (stats[z].perc[i] % 10 == 2 && stats[z].perc[i] != 12)
			fprintf(stdout, "%dnd percentile: %g\n", stats[z].perc[i],
				quartile_perc[i]);
		    else if (stats[z].perc[i] % 10 == 3 && stats[z].perc[i] != 13)
			fprintf(stdout, "%drd percentile: %g\n", stats[z].perc[i],
				quartile_perc[i]);
		    else
			fprintf(stdout, "%dth percentile: %g\n", stats[z].perc[i],
				quartile_perc[i]);
		}
	    }
	    G_free((void *)quartile_perc);
	    G_free((void *)qpos_perc);
	}

	/* G_message() prints to stderr not stdout: disabled. this \n is printed above with zone */
	/* if (!(param.shell_style->answer))
	    G_message("\n"); */
    }

    return 1;
}
