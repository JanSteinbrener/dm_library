/* This is the file dm_array.h */

#ifndef DM_ARRAY_H
#define DM_ARRAY_H

#include "dm.h"

/*#ifdef MPI
  #include <mpi.h>
  #else 
  If we aren't using MPI code, then this is superfluous but we'll
  define the type anyway. 
  typedef int MPI_Comm;
  #endif USE_MPI */

#ifdef __cplusplus
extern "C"
{
#endif /* __cplusplus */
  
#define DM_ARRAY_FORWARD_FFT (1<<5)
#define DM_ARRAY_INVERSE_FFT (1<<6)
#define DM_ARRAY_CREATE_FFT_PLAN (1<<1)
#define DM_ARRAY_DESTROY_FFT_PLAN (1<<2)
#define DM_ARRAY_FFT_PATIENT (0) /* Default */
#define DM_ARRAY_FFT_MEASURE (1<<3)
#define DM_ARRAY_FFT_ESTIMATE (1<<4) 
#define DM_ARRAY_STRLEN 80
  
  
  
    /** This routine copies a complex array from source to destination */
    void dm_array_copy_complex(dm_array_complex_struct *ptr_cas_dest, 
                               dm_array_complex_struct *ptr_cas_src);
    
    /** This routine copies a complex array from source to destination */
    void dm_array_copy_real(dm_array_real_struct *ptr_ras_dest, 
                            dm_array_real_struct *ptr_ras_src);

  /** This routine reassigns an existing ALLOCATED real structure to 
   * a different existing ALLOCATED structure. It will also copy all 
   * structure tags. This is meant as a convenience for the programmer 
   * if you need to change the size of an array but you want to keep the
   * same name for the structure. Be VERY CAREFUL using this routine!!
   * Potential pitfalls:
   * - existing memory in ptr_ras_dest will be lost
   * - both ptr_ras_dest and ptr_ras_src will reference the same memory 
   *   afterwards.
   */
  void dm_array_reassign_real(dm_array_real_struct *ptr_ras_dest,
			      dm_array_real_struct *ptr_ras_src,
			      int allocated);

  /** This routine takes a 2d real array structure and crops it to be 
   * square and centered around the offsets given. If dist_fft is used 
   * it will also make sure that the resulting dimensions are integer 
   * powers of 2. NOTE: uses dm_array_reassign_real to get the job done.
   */
  void dm_array_crop_2d_real(dm_array_real_struct *ptr_ras,
			     int xoffset, int yoffset,
			     int my_rank, int p);

    /** This routine replaces the magnitudes of a complex array with the ones 
     * provided in the real array. By also providing an array of ABSOLUTE
     * uncertainties for the magnitudes the routine will only replace 
     * magnitudes that do not lie in the interval [value-error,value+error]. 
     * If you do not
     * want to use errors, pass a NULL pointer as third argument.If you want
     * it to set all unknown magnitudes to 0 pass a non-zero value for 
     * zero_if_not_known
     */
    void dm_array_transfer_magnitudes(dm_array_complex_struct *ptr_cas_dest, 
                                      dm_array_real_struct *ptr_ras_mags,
                                      dm_array_real_struct *ptr_ras_errors,
				      int zero_if_not_known);
    
    /** This routine subtracts the second real array from
        the first real array
    */
    void dm_array_subtract_real(dm_array_real_struct *ptr_ras_diff,
                                dm_array_real_struct *ptr_ras);
    
    /** This routine adds the second real array to the first real array */
    void dm_array_add_real(dm_array_real_struct *ptr_ras_sum,
                           dm_array_real_struct *ptr_ras);
    
    /** This routine adds a real scalar into a complex array */
    void dm_array_add_real_scalar(dm_array_complex_struct *ptr_cas,
                                  dm_array_real scalar_value);
  
    /** This routine subtracts the second complex array
        from the first complex array
    */
    void dm_array_subtract_complex(dm_array_complex_struct *ptr_cas_diff,
                                   dm_array_complex_struct *ptr_cas);
    
    /** This routine adds the second complex array
        to the first complex array
    */
    void dm_array_add_complex(dm_array_complex_struct *ptr_cas_sum,
                              dm_array_complex_struct *ptr_cas);
    
    /** This routine adds a complex scalar into a complex array */
    void dm_array_add_complex_scalar(dm_array_complex_struct *ptr_cas, 
                                     dm_array_complex *ptr_scalar_value);
  
    /** This routine multiplies a complex scalar into a complex array */
    void dm_array_multiply_real_scalar(dm_array_complex_struct *ptr_cas, 
                                       dm_array_real scalar_value);
  
    /** This routine multiplies a complex scalar into a complex array */
    void dm_array_multiply_complex_scalar(dm_array_complex_struct *ptr_cas, 
                                          dm_array_complex *ptr_scalar_value);
  
    /** This routine puts the real part of complex_array into real_array */
    void dm_array_realpart(dm_array_real_struct *real_array,
                           dm_array_complex_struct *complex_array);
  
    /** This routine puts the imaginary part of complex_array
        into real_array
    */
    void dm_array_imaginarypart(dm_array_real_struct *real_array,
                                dm_array_complex_struct *complex_array);
  
  /** This routine sums up over all complex elements and stores the sum 
      in ptr_complex_sum. If only one array is given it will store the 
      square of the sum of complex values, if two arrays are given it 
      will store the sum of array1(i)*conj(array2(i)).
  */
  void dm_array_square_sum_complex(dm_array_complex_struct *ptr_cas,
				   dm_array_complex_struct *ptr_c_cas,
				   dm_array_complex *ptr_complex_sum);

    /** This routine puts the magnitude of complex_array into real_array */
    void dm_array_magnitude_complex(dm_array_real_struct *real_array,
                                    dm_array_complex_struct *complex_array);

    /** This routine puts the magnitude of a real_array
        into magnitude_array. If the user sets the is_intensities flag
        it will also take the sqrt of each value.
    */
    void dm_array_magnitude_real(dm_array_real_struct *magnitude_array,
                                 dm_array_real_struct *real_array,
                                 int is_intensities);
    
    /** This routine puts the phase of complex_array into real_array */
    void dm_array_phase(dm_array_real_struct *real_array,
                        dm_array_complex_struct *complex_array);

    /** This routine returns the global phase of complex_array */
    dm_array_real dm_array_global_phase(dm_array_complex_struct *complex_array);

    /** This routine puts the intensity (square) of complex_array 
        into real_array */
    void dm_array_intensity(dm_array_real_struct *real_array,
                            dm_array_complex_struct *complex_array);
  
    /** This routine zeroes a complex array */
    void dm_array_zero_complex(dm_array_complex_struct *ptr_cas);

    /** This routine zeroes a real array */
    void dm_array_zero_real(dm_array_real_struct *ptr_ras);
  
  /** This routine calculates the total power of a complex amplitude 
      array, by adding up the squares of all the real and imaginary
      parts. If ptr_indices is provided then it will only calculate 
      total power from indices where ptr_indices == 1 (0 if inverse is
      1).
  */
  dm_array_real dm_array_total_power_complex(dm_array_complex_struct *ptr_cas,
					     dm_array_byte_struct *ptr_indices,
					     int inverse);

  /** This routine calculates the total power of a real amplitude
      array, by adding up the squares of all elements or just the
      elements if we are dealing with intensities
  */
  dm_array_real dm_array_total_power_real(dm_array_real_struct *ptr_ras,
					  int is_intensities);

  /** This routine calculates the maximum of a given real array.
   */
  dm_array_real dm_array_max_real(dm_array_real_struct *ptr_ras,
				  int p);
  
  /** This routine calculates the minimum of a given real array.
   */  
  dm_array_real dm_array_min_real(dm_array_real_struct *ptr_ras,
				  int p);


    /* This routine will fill the real part of a
       complex array with random numbers.
    **/
  void dm_array_rand(dm_array_complex_struct *ptr_cas, 
		     int imaginary_too);
    
    /** This routine fills in the array with a pure real gaussian
        function with a center at [(nx/2),(ny/2),(nz/2)].  If the 
        array is 1D or 2D, the values of sigma_y and sigma_z are 
        ignored as appropriate.
    */
    void dm_array_load_gaussian(dm_array_complex_struct *ptr_cas,
                                dm_array_real sigma_x,
                                dm_array_real sigma_y,
                                dm_array_real sigma_z,
                                int inverse,
				int fft_centered,
                                int p,
                                int my_rank);
  
    /** This routine multiplies two complex arrays element by element. 
        The first array will hold the result of the computation
    */
    void dm_array_multiply_complex(dm_array_complex_struct *ptr_cas_one,
                                   dm_array_complex_struct *ptr_cas_two);

    /** This routine calculates the scalar product of a
        complex with a byte array. The first array will hold
        the result of the computation
    */
    void dm_array_multiply_complex_byte(dm_array_complex_struct *ptr_cas,
                                        dm_array_byte_struct *ptr_bas);

    /** This routine does an FFT on a complex array.  It uses the
        FFTW routines by default unless you specified -DDIST_FFT at
        compile time, in which case it uses the Apple dist_fft routines.
        The routine remembers array dimensions from the last time it
        was called, so it takes care of making a new plan or simply
        re-using the existing plan for you.  It also calls 1D, 2D,
        or 3D routines as appropriate based on looking at nx, ny, and nz.
        It takes care of normalization so that energy is preserved, and
        it works on the assumption that the center of the image is
        at [(nx/2),(ny/2),(nz/2)] in real space while the center in
        Fourier space is at [0,0,0].  
      
        You should first call this routine with
        fft_options=DM_ARRAY_CREATE_FFT_PLAN
        so that it builds a "plan" (the pre-calculated parameters
        and factorization of the FFT) for both forward and inverse
        transforms.  Creation of a plan involves destruction of the
        data in the complex array.  After you've loaded the data of
        interest into the array, you can then do as many forward and 
        inverse transforms as you like by setting fft_options to either
        DM_ARRAY_FORWARD_FFT or DM_ARRAY_INVERSE_FFT. If you
        are then going to change the array address or dimensions,
        you should first call this routine with 
        fft_options=DM_ARRAY_DESTROY_FFT_PLAN and then go through
        the plan creation process again.
      
        Note that if you have FFTs on a couple of different arrays,
        it is up to you to keep track of the different forward and
        backward plan files for each array.
      
        Finally, there are additional options that can be
        bit-combined with creating a plan: DM_ARRAY_FFT_PATIENT (the
        default), DM_ARRAY_FFT_MEASURE, or DM_ARRAY_FFT_ESTIMATE.
        ESTIMATE is fastest to plan and slowest to execute,
        followed by MEASURE and then PATIENT.
    */
    void dm_array_fft(dm_array_complex_struct *ptr_cas,
                      int p,
                      int fft_options,
                      int rank);
  
#ifdef __cplusplus
}  /* extern "C" */
#endif /* __cplusplus */

#endif /* #ifndef DM_ARRAY_H */
