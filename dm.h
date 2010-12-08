/* This is the file dm.h */

#ifndef DM_H
#define DM_H

#include <stdio.h> 
#include <math.h>
#include <string.h>
#include <stdlib.h>
#include <png.h>
#include <time.h>
#include <sys/times.h>
#include <sys/time.h>

#ifdef __MPI__

#include <mpi.h>
#define USE_MPI 1
typedef double dm_time_t;

#else
  #define USE_MPI 0
  /* Both mpi and dist_fft will include mpi.h */
  #ifndef DIST_FFT
    typedef int MPI_Comm;
  #endif
  typedef clock_t dm_time_t;
#endif /*__MPI__*/

#include <sys/types.h>

#if (defined(__APPLE__) || defined(__CYGWIN__))
#include <machine/endian.h>
#else
#include <endian.h>
#endif

#if defined(__APPLE__)
#include <sys/malloc.h>
#else
#include <malloc.h>
#endif

#ifdef __cplusplus
extern "C"
{
#endif /* __cplusplus */

/** You have to decide at compile time whether all arrays will
    be single or double precision.  To work in double precision,
    define DM_ARRAY_DOUBLE.
*/
#ifdef DM_ARRAY_DOUBLE
typedef double dm_array_real;
#define MPI_ARRAY_REAL MPI_DOUBLE
#else
typedef float dm_array_real;
#define MPI_ARRAY_REAL MPI_FLOAT
#endif

/* Define an real array allocation macro that takes care
 * of whether we are using MPI or not.
 */
#define DM_ARRAY_REAL_STRUCT_INIT(__struct,__npixels,__np) {		\
    (__struct->real_array) =						\
      (dm_array_real *)malloc(sizeof(dm_array_real)*(__npixels/__np));	\
    __struct->local_npix = __npixels/__np;				\
  }

#define DM_ARRAY_BYTE_STRUCT_INIT(__struct,__npixels,__np) {		\
    (__struct->byte_array) = (u_int8_t *)malloc(sizeof(u_int8_t)*	\
						(__npixels/__np));	\
    __struct->local_npix = __npixels/__np;				\
  }

#define DM_ARRAY_INT_STRUCT_INIT(__struct,__npixels,__np) {		\
    (__struct->int_array) = (int *)malloc(sizeof(int)*			\
					   (__npixels/__np));		\
    __struct->local_npix = __npixels/__np;				\
  }

/*--------------------------------------------------------*/
#if defined(DIST_FFT)
#include "dist_fft.h" /* This also defines the macros c_re and c_im */
typedef dist_fft_complex dm_array_complex;
typedef dist_fft_storage dm_fft_storage;
typedef dist_fft_plan dm_fft_plan;
#define DM_ARRAY_COMPLEX_STRUCT_INIT(__struct,__npixels,__np) {		\
    DIST_FFT_MALLOC_DATA(__struct->complex_array,(__npixels/__np));	\
    __struct->local_npix = __npixels/__np;				\
}

#define DM_ARRAY_COMPLEX_MALLOC(__storage,__npixels) {		\
    DIST_FFT_MALLOC_DATA(__storage,__npixels);			\
  }

#define DM_ARRAY_COMPLEX_FREE(__storage)        \
    DIST_FFT_FREE_DATA(__storage) 

 /* Dist_fft lets you choose between split or interleaved.
  * This has to be taken into account when doing MPI.
  */ 
  #ifdef DIST_FFT_USE_INTERLEAVED_COMPLEX
  #define DM_ARRAY_INTERLEAVED 1
  #define DM_ARRAY_SPLIT 0
  #else
  #define DM_ARRAY_SPLIT 1
  #define DM_ARRAY_INTERLEAVED 0
  #endif
#else 
/*--------------------------------------------------------*/
/** This is the case of using the FFTW3 routines.  We define
    the same sort of c_re() and c_im() macros to be compatible
    with dist_fft.
*/
#include "fftw3.h"

#if defined(DM_ARRAY_DOUBLE)
typedef fftw_plan dm_fft_plan;
typedef fftw_complex dm_array_complex;
#else
typedef fftwf_plan dm_fft_plan;
typedef fftwf_complex dm_array_complex;
#endif
typedef dm_array_complex *dm_fft_storage;

/* by default fftw_complex creates interleaved arrays */
#define c_re(c, index)  (c[index][0])
#define c_im(c, index)  (c[index][1])

#if defined(DM_ARRAY_DOUBLE)
/* Because dm_array_complex might be a struct of pointers,
 * we should not use it for sizeof(). */
#define DM_ARRAY_COMPLEX_STRUCT_INIT(__struct,__npixels,__np) {         \
        __struct->complex_array =                                        \
            fftw_malloc(2*sizeof(dm_array_real)*__npixels/__np);        \
        __struct->local_npix = __npixels/__np;                           \
    }

#define DM_ARRAY_COMPLEX_MALLOC(__storage,__npixels) {                  \
        __storage =	 fftw_malloc(2*sizeof(dm_array_real)*__npixels); \
    }

#define DM_ARRAY_COMPLEX_FREE(__storage) {      \
        fftw_free((__storage));                 \
    }
#else
/* Because dm_array_complex might be a struct of pointers,
 * we should not use it for sizeof(). */
#define DM_ARRAY_COMPLEX_STRUCT_INIT(__struct,__npixels,__np) {		\
        (__struct->complex_array) =                                      \
            fftwf_malloc(2*sizeof(dm_array_real)*__npixels/__np);       \
        __struct->local_npix = __npixels/__np;				\
  }

#define DM_ARRAY_COMPLEX_MALLOC(__storage,__npixels) {		\
    __storage =	 fftwf_malloc(2*sizeof(dm_array_real)*__npixels);	\
  }

#define DM_ARRAY_COMPLEX_FREE(__storage) {      \
        fftwf_free((__storage));                \
    }
#endif /* DM_ARRAY_DOUBLE */

/* fftw3 only offers interleaved arrays */
#define DM_ARRAY_INTERLEAVED 1
#define DM_ARRAY_SPLIT 0
#endif /* DIST_FFT */

#ifndef NULL
#define NULL 0
#endif


/* Define array indices to be of type dm_array_index_t, so that
   if we ever have to go to 64 bit array indices all we have to
   do is to change the following line. */
typedef u_int32_t dm_array_index_t;

typedef struct { 
  dm_array_complex *complex_array; 
  int nx;
  int ny;
  int nz;
  dm_array_index_t npix;
  dm_array_index_t local_npix;
  dm_array_index_t local_offset;
  dm_fft_plan ptr_forward_plan;
  dm_fft_plan ptr_inverse_plan;
} dm_array_complex_struct;

typedef struct { 
  dm_array_real *real_array; 
  int nx;
  int ny;
  int nz;
  dm_array_index_t npix;
  dm_array_index_t local_npix;
  dm_array_index_t local_offset;
} dm_array_real_struct;

typedef struct { 
  u_int8_t *byte_array; 
  int nx;
  int ny;
  int nz;
  dm_array_index_t npix;
  dm_array_index_t local_npix;
  dm_array_index_t local_offset;
} dm_array_byte_struct;

typedef struct { 
  int *int_array; 
  int nx;
  int ny;
  int nz;
  dm_array_index_t npix;
  dm_array_index_t local_npix;
  dm_array_index_t local_offset;
} dm_array_int_struct;


#define DM_ADI_VERSION 1
typedef struct {
  double photon_scaling;
  double error_scaling;
  double lambda_meters;
  double camera_z_meters;
  double camera_x_pixelsize_meters;
  double camera_y_pixelsize_meters;
  double camera_alpha_radians;
  double camera_beta_radians;
  double camera_gamma_radians;
  double median_filter_width;
  double median_filter_threshold;
  double saturation_min;
  double saturation_max;
  double theta_x_radians;
  double theta_y_radians;
  double theta_z_radians;
  double xcenter_offset_pixels;
  double ycenter_offset_pixels;
} dm_adi_struct;


/* Ainfo_struct: Contains offset variables for every
 * array and a flag for the file_directory. 
 * This way the functions should still work even
 * if the user did not define an array.
 */
#define DM_AINFO_VERSION 1
#define AINFO_TAGS 11
typedef struct {
  char *file_directory;
  char *filename_array;
  char *systime_array;
  double *theta_x_radians_array;
  double *xcenter_offset_pixels_array;
  double *ycenter_offset_pixels_array;
  int file_directory_flag;
  int filenames_offset;
  int systimes_offset;
  int theta_x_offset;
  int xcenter_offset;
  int ycenter_offset;
  int n_frames;
  int n_frames_max;
  int string_length;
  int ainfo_tags;
  int no_background;
  int dk_by_pix;
  int merge_first;
} dm_ainfo_struct;

#define DM_SPT_VERSION 1
typedef struct {
  double support_scaling;
  double pix_x_meters;
  double pix_y_meters;
  double pix_z_meters;
} dm_spt_struct;

#define DM_ITN_VERSION 1
typedef struct {
  double photon_scaling;
  double pix_x_meters;
  double pix_y_meters;
  double pix_z_meters;
  u_int32_t iterate_count;
} dm_itn_struct;

typedef struct {
  char *string_array;
  char *specimen_name;
  char *collection_date;
  int n_strings;
  int n_strings_max;
  int string_length;
} dm_comment_struct;

/* This routine is called to initialize both the dm_array and
 * the dm_fileio libraries. It will initialize MPI if necessary
 * and supply the correct values for my_rank and p
 */
void dm_init(int *p, 
             int *this_rank);

/** This routine will finalize the MPI session if we are using MPI */
void dm_exit();

/** This routine is a random number generator following the minimal
    standard guidelines described in Sec. 7.1 of Numerical Recipes
 */
dm_array_real dm_rand(long *idum);

/** These routines are for timing purposes */
void dm_time(dm_time_t *time);
double dm_time_diff(dm_time_t start,
                    dm_time_t stop);

/* This routine will create a png output from the real array structure */
int dm_write_png(dm_array_real_struct *ptr_ras,
		 char *filename,
		 char *error_string,
		 int log_scale,
		 int p,
		 int my_rank);


#ifdef __cplusplus
}  /* extern "C" */
#endif /* __cplusplus */

#endif /* #ifndef DM_H */
