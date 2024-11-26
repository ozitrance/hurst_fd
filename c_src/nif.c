#include "erl_nif.h"
#include <math.h>
#include <time.h>

static inline int rand_int(int min, int max) {
    return min + rand() % (max - min);
}

static ERL_NIF_TERM compute_rs(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {

    if (argc != 3) return enif_make_badarg(env);

    ErlNifIOVec *iovec = NULL;
    ERL_NIF_TERM tail;

    unsigned int num_samples;
    unsigned int window_sizes_len;
    if (
        !enif_inspect_iovec(env, 0, argv[0], &tail, &iovec)
        || !enif_get_uint(env, argv[1], &num_samples)
        || !enif_get_list_length(env, argv[2], &window_sizes_len)
        ) {
        return enif_make_badarg(env);
    }


    size_t len_log_returns = iovec->size / sizeof(double); 

    srand((unsigned int)time(NULL));

    ERL_NIF_TERM result;
    unsigned char* out_data_raw = enif_make_new_binary(env, window_sizes_len * sizeof(double), &result);
    double* out_data = (double*)out_data_raw; 

    size_t segment_count = iovec->iovcnt;
    double** segment_data = (double**)enif_alloc(segment_count * sizeof(double*));
    size_t* segment_lengths = (size_t*)enif_alloc(segment_count * sizeof(size_t));

    for (size_t i = 0; i < segment_count; i++) {
        segment_data[i] = (double*)iovec->iov[i].iov_base;
        segment_lengths[i] = iovec->iov[i].iov_len / sizeof(double);
    }

    ERL_NIF_TERM windows_list_iter = argv[2];
    ERL_NIF_TERM list_head, list_tail;
    unsigned int i = 0;
    while (enif_get_list_cell(env, windows_list_iter, &list_head, &list_tail)) {
        int w;
        if (!enif_get_int(env, list_head, &w)) {
            return enif_make_atom(env, "error");
        }

        double sum_R = 0.0;
        double sum_S = 0.0;

        for (int sample = 0; sample < num_samples; sample++) {
            size_t start = rand_int(0, len_log_returns - w);
            double max_seq = -INFINITY;
            double min_seq = INFINITY;
            double sum_seq = 0.0;
            double sum_sq_diff = 0.0;

            size_t idx = start;
            size_t seg_idx = 0;
            size_t seg_offset = 0;

            size_t cumulative_len = 0;
            for (; seg_idx < segment_count; seg_idx++) {
                if (idx < cumulative_len + segment_lengths[seg_idx]) {
                    seg_offset = idx - cumulative_len;
                    break;
                }
                cumulative_len += segment_lengths[seg_idx];
            }

            // Compute mean
            size_t remaining = w;
            size_t curr_seg_idx = seg_idx;
            size_t curr_seg_offset = seg_offset;

            while (remaining > 0 && curr_seg_idx < segment_count) {
                size_t seg_len = segment_lengths[curr_seg_idx];
                size_t offset = curr_seg_offset;
                size_t to_read = seg_len - offset;
                if (to_read > remaining) to_read = remaining;

                double* data = segment_data[curr_seg_idx] + offset;
                for (size_t j = 0; j < to_read; j++) {
                    double val = data[j];
                    sum_seq += val;
                }

                remaining -= to_read;
                curr_seg_idx++;
                curr_seg_offset = 0;
            }

            double mean_seq = sum_seq / w;

            // Compute min, max, and standard deviation
            remaining = w;
            curr_seg_idx = seg_idx;
            curr_seg_offset = seg_offset;

            while (remaining > 0 && curr_seg_idx < segment_count) {
                size_t seg_len = segment_lengths[curr_seg_idx];
                size_t offset = curr_seg_offset;
                size_t to_read = seg_len - offset;
                if (to_read > remaining) to_read = remaining;

                double* data = segment_data[curr_seg_idx] + offset;
                for (size_t j = 0; j < to_read; j++) {
                    double val = data[j];
                    if (val > max_seq) max_seq = val;
                    if (val < min_seq) min_seq = val;
                    double diff = val - mean_seq;
                    sum_sq_diff += diff * diff;
                }

                remaining -= to_read;
                curr_seg_idx++;
                curr_seg_offset = 0;
            }

            double std_seq = sqrt(sum_sq_diff / w);

            double R = max_seq - min_seq;
            double S = std_seq;


            sum_R += R;
            sum_S += S;

        }

        // mean R and S
        double mean_R = sum_R / num_samples;
        double mean_S = sum_S / num_samples;
        double rs_value = mean_R / mean_S;

        out_data[i] = rs_value;

        i++;
        windows_list_iter = list_tail;

    }

    return result;
}

static ErlNifFunc nif_funcs[] = {
    {"compute_rs", 3, compute_rs, ERL_NIF_DIRTY_JOB_CPU_BOUND}
};

ERL_NIF_INIT(Elixir.HurstFdNif, nif_funcs, NULL, NULL, NULL, NULL)
