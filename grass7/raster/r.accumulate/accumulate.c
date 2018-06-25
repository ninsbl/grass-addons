#include "global.h"

double accumulate(struct cell_map *dir_buf, struct raster_map *weight_buf,
                  struct raster_map *accum_buf, char **done, char neg,
                  int row, int col)
{
    int rows = dir_buf->rows, cols = dir_buf->cols;
    int i, j;
    char incomplete = 0;
    double accum;

    /* if the current cell has been calculated, just return its accumulation so
     * that downstream cells can simply propagate and add it to themselves */
    if (done[row][col]) {
        accum = get(accum_buf, row, col);

        /* for negative accumulation, always return its absolute value;
         * otherwise return it as is */
        return neg && accum < 0 ? -accum : accum;
    }

    /* if a weight map is specified (no negative accumulation is implied), use
     * the weight value at the current cell; otherwise use 1 */
    if (weight_buf->map.v)
        accum = get(weight_buf, row, col);
    else
        accum = 1.0;

    /* loop through all neighbor cells and see if any of them drains into the
     * current cell (are there upstream cells?) */
    for (i = -1; i <= 1; i++) {
        /* if a neighbor cell is outside the computational region, its
         * downstream accumulation is incomplete */
        if (row + i < 0 || row + i >= rows) {
            incomplete = 1;
            continue;
        }

        for (j = -1; j <= 1; j++) {
            /* skip the current cell */
            if (i == 0 && j == 0)
                continue;

            /* if a neighbor cell is outside the computational region, its
             * downstream accumulation is incomplete */
            if (col + j < 0 || col + j >= cols) {
                incomplete = 1;
                continue;
            }

            /* if a neighbor cell drains into the current cell and the current
             * cell doesn't flow back into the same neighbor cell (no flow
             * loop), trace and recursively accumulate upstream cells */
            if (dir_buf->c[row + i][col + j] == dir_checks[i + 1][j + 1][0] &&
                dir_buf->c[row][col] != dir_checks[i + 1][j + 1][1]) {
                /* for negative accumulation, accumulate() always returns a
                 * positive value, so accum is always positive (cell count);
                 * otherwise, accum is weighted accumulation */
                accum +=
                    accumulate(dir_buf, weight_buf, accum_buf, done, neg,
                               row + i, col + j);

                /* if the neighbor cell is incomplete, the current cell also
                 * becomes incomplete */
                if (done[row + i][col + j] == 2)
                    incomplete = 1;
            }
        }
    }

    /* if negative accumulation is desired and the current cell is incomplete,
     * use a negative cell count without weighting; otherwise use accumulation
     * as is (cell count or weighted accumulation, which can be negative) */
    set(accum_buf, row, col, neg && incomplete ? -accum : accum);

    /* the current cell is done; 1 for no likely underestimates and 2 for
     * likely underestimates */
    done[row][col] = 1 + incomplete;

    /* for negative accumulation, accum is already positive */
    return accum;
}
