#include <stdio.h>
#include "../dm.h"
#include "../dm_array.h"
#include <stdlib.h>


void dm_test_array_help() {
  
  printf("Usage: dm_test_array [-wait -fftonly -d x -nd y -nf z]\n");
  printf("  -wait: Endless loop for debugging parallel. Set debugWait = 0.\n");
  printf("  -d x: make the dimension be of size x. \n");
  printf("  -nd y: make the array have y dimensions.\n");
  printf("  -nf z: Perform z FFT pairs. \n");
  printf("  -fftonly: Test only ffts. \n");
}


/*-------------------------------------------------------------*/
main(int argc, char **argv) {
  char this_arg[128], error_string[128];
  dm_array_complex_struct array_2d_cas, copied_array;
  dm_array_real_struct intens_array, real_array;
  dm_array_byte_struct byte_array;
  int my_rank, p, i, nffts, j, i_arg;
  int nx,n_dims,is_fftonly;
  dm_array_real power_before, max_value;
  dm_array_complex *value_to_add;
  dm_array_complex *multipl_value; /* Compiler needs size of structures */
  float temp_re, temp_im;
  int print_limit, debugWait;
  dm_time_t te, ts, te_total, ts_total;
  double tdelta;


  dm_init(&p,&my_rank);
  
  /* define hardwired defaults */
  print_limit = 5;
  i_arg = 1;
  
  /* define defaults that user can change through CLA */
  nx = 64;
  n_dims = 2;
  debugWait = 0;
  nffts = 10;
  is_fftonly = 0;
  
  while (i_arg < argc) {
      strcpy( this_arg, argv[i_arg] );
      if ((strncasecmp("-?",this_arg,2) == 0) || 
          (strncasecmp("-H",this_arg,2) == 0)) {
	dm_test_array_help();
	exit(1);
      } else if (strncasecmp("-W",this_arg,2) == 0) {
	debugWait = 1;
	i_arg++;
      } else if (strncasecmp("-ND",this_arg,3) == 0) {
	sscanf(argv[i_arg+1],"%d",&n_dims);
	i_arg = i_arg+2;
      } else if (strncasecmp("-D",this_arg,2) == 0) {
	sscanf(argv[i_arg+1],"%d",&nx);
	i_arg = i_arg+2;
      } else if (strncasecmp("-NF",this_arg,3) == 0) {
	sscanf(argv[i_arg+1],"%d",&nffts);
	i_arg = i_arg+2;      
      } else if (strncasecmp("-f",this_arg,2) == 0) {
	is_fftonly = 1;
	i_arg++;
      } else {
	i_arg++;
      }
  }
  
  while (debugWait);

  if (n_dims == 1) {
      array_2d_cas.nx = nx;
      array_2d_cas.ny = 1;
      array_2d_cas.nz = 1;
  } else if (n_dims == 2) {
      array_2d_cas.nx = nx;
      array_2d_cas.ny = nx;
      array_2d_cas.nz = 1;
  } else if (n_dims == 3) {
      array_2d_cas.nx = nx;
      array_2d_cas.ny = nx;
      array_2d_cas.nz = nx;
  } else {
      printf("Who cares about %d dimensions?!\n");
      exit(1);
  }
  
  array_2d_cas.npix = array_2d_cas.nx*array_2d_cas.ny*array_2d_cas.nz;

  if (!is_fftonly) {
      copied_array.nx = array_2d_cas.nx;
      copied_array.ny = array_2d_cas.ny;
      copied_array.nz = array_2d_cas.nz;
      copied_array.npix = array_2d_cas.npix;
      
      real_array.nx = array_2d_cas.nx;
      real_array.ny = array_2d_cas.ny;
      real_array.nz = array_2d_cas.nz;
      real_array.npix = array_2d_cas.npix;
      
      intens_array.nx = array_2d_cas.nx;
      intens_array.ny = array_2d_cas.ny;
      intens_array.nz = array_2d_cas.nz;
      intens_array.npix = array_2d_cas.npix;
      
      byte_array.nx = array_2d_cas.nx;
      byte_array.ny = array_2d_cas.ny;
      byte_array.nz = array_2d_cas.nz;
      byte_array.npix = array_2d_cas.npix;
  }
  printf("my rank: %d, p: %d\n",my_rank, p);

  printf("Size of the array: %d x %d x %d = %d\n",array_2d_cas.nx,
         array_2d_cas.ny,array_2d_cas.nz,array_2d_cas.npix);
  DM_ARRAY_COMPLEX_STRUCT_INIT((&array_2d_cas), array_2d_cas.npix,p);
  DM_ARRAY_COMPLEX_MALLOC(value_to_add,1);
  
  dm_array_zero_complex(&array_2d_cas);

  /* Initialize the array with random real part */
  dm_array_rand(&array_2d_cas);
  printf("After initializing with random values from rank %d: \n",my_rank);
  for (i = 0; i < print_limit; i++) {
      temp_re = c_re(array_2d_cas.complex_array,i);
      temp_im = c_im(array_2d_cas.complex_array,i);
      printf("{%f,%f} ", temp_re,temp_im);
  }
  printf("\n");
  
  /* Add a complex scalar */
  c_re(value_to_add,0) = .00001;
  c_im(value_to_add,0) = -2.;
  printf("scalar to add: {%f, %f}.\n",c_re(value_to_add,0),
	 c_im(value_to_add,0));

  dm_array_add_complex_scalar(&array_2d_cas,value_to_add); 
  printf("After add complex\n"); 
  for (i = 0; i < print_limit; i++) {
      temp_re = c_re(array_2d_cas.complex_array,i);
      temp_im = c_im(array_2d_cas.complex_array,i);
      printf("{%f,%f} ", temp_re,temp_im);
  }
  printf("\n");

  
  if (!is_fftonly) {
    DM_ARRAY_COMPLEX_STRUCT_INIT((&copied_array), copied_array.npix,p);
    DM_ARRAY_REAL_STRUCT_INIT((&real_array),real_array.npix,p);
    DM_ARRAY_REAL_STRUCT_INIT((&intens_array),intens_array.npix,p);
    DM_ARRAY_BYTE_STRUCT_INIT((&byte_array),byte_array.npix,p);

      /* Need to allocate memory for complex numbers - two possibilities:
       *   1. Let every process allocate for itself -> more memory used,
       *      but no special treatment necessary
       *   2. Only root allocates memory, then broadcast address to 
       *      processes -> less memory used, but additional message and 
       *      some if statements required.
       *   ==> For now go with option 1!
       */
      DM_ARRAY_COMPLEX_MALLOC(multipl_value,1);
      
    
      /* Load an array and add a complex number */
      dm_array_zero_complex(&copied_array);
      dm_array_zero_real(&real_array);
      dm_array_zero_real(&intens_array);
      
      /* For the support: Alternating zero and one */
      for (i = 0; i < array_2d_cas.npix/p; i++) {
          *(byte_array.byte_array+i) = (u_int8_t)(i%2);
      }
      
      /* For the real array a ramp pattern */
      for (i = 0; i < real_array.npix/p; i++) {
          *(real_array.real_array+i) = (dm_array_real)(my_rank+1.*i);
      }
      
      printf("Created real array as seen from rank %d: \n",my_rank);
      for (i = 0; i < print_limit; i++) {
          temp_re =  *(real_array.real_array+i);
          printf("%f, ", temp_re);
      }
      printf("\n");
      
      /* Test real array copying */
      dm_array_copy_real(&intens_array,&real_array);
      printf("The copied array seen from rank %d: \n",my_rank);
      for (i = 0; i < print_limit; i++) {
          temp_re =  *(intens_array.real_array+i);
          printf("%f, ", temp_re);
      }
      printf("\n");
      
      /* Test adding real arrays */
      dm_array_add_real(&real_array,&intens_array);
      printf("After adding real array as seen from rank %d: \n",my_rank);
      for (i = 0; i < print_limit; i++) {
          temp_re =  *(real_array.real_array+i);
          printf("%f, ", temp_re);
      }
      printf("\n");
      
      /* Test subtracting real arrays */
      dm_array_subtract_real(&intens_array,&real_array);
      printf("After subtracting real array as seen from rank %d: \n",my_rank);
      for (i = 0; i < print_limit; i++) {
          temp_re =  *(intens_array.real_array+i);
          printf("%f, ", temp_re);
      }
      printf("\n");
      
      /* Test real magnitude */
      dm_array_magnitude_real(&real_array, &intens_array,1);
      printf("Extracted magnitude of real array from rank %d: \n", my_rank);
      for (i = 0; i < print_limit; i++) {
          temp_re =  real_array.real_array[i];
          printf("%f ", temp_re);
      }
      printf("\n");

      /* Test total power of real array */
      power_before = dm_array_total_power_real(&intens_array,0)/
          ((double)intens_array.npix);
      if (my_rank == 0) {
          printf("   Average power of real array: %.6f\n",
                 (dm_array_real)power_before);
      } 

      /* Test maximum value of real array */
      max_value = dm_array_max_real(&real_array,p);
      if (my_rank == 0) {
          printf("   Maximum value of real array: %.6f\n",
                 (dm_array_real)max_value);
      } 

      /* Now zero both real array */
      dm_array_zero_real(&real_array);
      dm_array_zero_real(&intens_array);
      
      /* Load a Gaussian */
      dm_array_load_gaussian(&copied_array,30.,30.,30.,0,0,p,my_rank);
      printf("After loading a Gaussian from rank %d: \n",my_rank);
      for (i = 0; i < print_limit; i++) {
          temp_re = c_re(copied_array.complex_array,i);
          temp_im = c_im(copied_array.complex_array,i);
          printf("{%f,%f} ", temp_re,temp_im);
      }
      printf("\n");
      dm_array_zero_complex(&copied_array);
      
      dm_time(&ts);
      
      /* Test total power */
      power_before = dm_array_total_power_complex(&array_2d_cas,NULL,0)/
          ((double)array_2d_cas.npix);
      if (my_rank == 0) {
          printf("   Average power: %.6f\n", (dm_array_real)power_before);
      } 
      
      /* Test multiply_real */
      dm_array_multiply_real_scalar(&array_2d_cas, (dm_array_real)2);
      printf("After multiplying real from rank %d: \n",my_rank);
      for (i = 0; i < print_limit; i++) {
          temp_re =  c_re(array_2d_cas.complex_array,i);
          temp_im = c_im(array_2d_cas.complex_array,i);
          printf("{%f,%f} ", temp_re,temp_im);
      }
      printf("\n");
      
      /* Test add real */
      dm_array_add_real_scalar(&array_2d_cas, (dm_array_real)1.0);
      printf("After adding real from rank %d: \n",my_rank);
      for (i = 0; i < print_limit; i++) {
          temp_re =  c_re(array_2d_cas.complex_array,i);
          temp_im = c_im(array_2d_cas.complex_array,i);
          printf("{%f,%f} ", temp_re,temp_im);
      }
      printf("\n");
      
      dm_time(&te);
      tdelta = dm_time_diff(ts,te);
      printf("Tdelta: %f\n",tdelta);
      
      /* Test multiply complex scalar */
      c_re(multipl_value,0) = -1.;
      c_im(multipl_value,0) = 2.;
      dm_array_multiply_complex_scalar(&array_2d_cas,multipl_value);
      printf("After multiplying with complex scalar from rank %d: \n",my_rank);
      for (i = 0; i < print_limit; i++) {
          temp_re =  c_re(array_2d_cas.complex_array,i);
          temp_im = c_im(array_2d_cas.complex_array,i);
          printf("{%f,%f} ", temp_re,temp_im);
      }
      printf("\n");
      
      /* Test realpart */
      dm_array_realpart(&real_array, &array_2d_cas);
      printf("Extracted realpart from rank %d: \n",my_rank);
      for (i = 0; i < print_limit; i++) {
          temp_re =  real_array.real_array[i];
          printf("%f ", temp_re);
      }
      printf("\n");
      
      /* Test imaginarypart */
      dm_array_imaginarypart(&real_array, &array_2d_cas);
      printf("Extracted imaginarypart from rank %d: \n",my_rank);
      for (i = 0; i < print_limit; i++) {
          temp_re =  real_array.real_array[i];
          printf("%f ", temp_re);
      }
      printf("\n");
      
      /* Test magnitude */
      dm_array_magnitude_complex(&real_array, &array_2d_cas);
      printf("Extracted magnitude from rank %d before: \n", my_rank);
      for (i = 0; i < print_limit; i++) {
          temp_re =  real_array.real_array[i];
          printf("%f ", temp_re);
      }
      printf("\n");
      
      /* Test phase */
      dm_array_phase(&real_array, &array_2d_cas);
      printf("Extracted phase from rank %d before: \n",my_rank);
      for (i = 0; i < print_limit; i++) {
          temp_re =  real_array.real_array[i];
          printf("%f ", temp_re);
      }
      printf("\n");

      /* Add something to the magnitudes and replace the existing ones */
      for (i = 0; i < real_array.npix/p; i++) {
          *(real_array.real_array+i) = (dm_array_real)(my_rank+10);
      }

      /* Let's make up an error array. */
      for (i = 0; i < intens_array.npix/p; i++) {
          *(intens_array.real_array+i) = (dm_array_real)0.5;
      }

      /* if not using errors, pass NULL pointer as third argument. */
      dm_array_transfer_magnitudes(&array_2d_cas,&real_array,&intens_array);
       
      /* Test magnitude after */
      dm_array_magnitude_complex(&real_array, &array_2d_cas);
      printf("Extracted magnitude from rank %d after: \n", my_rank);
      for (i = 0; i < print_limit; i++) {
          temp_re =  real_array.real_array[i];
          printf("%f ", temp_re);
      }
      printf("\n");
      
      /* Test phase after */
      dm_array_phase(&real_array, &array_2d_cas);
      printf("Extracted phase from rank %d after: \n",my_rank);
      for (i = 0; i < print_limit; i++) {
          temp_re =  real_array.real_array[i];
          printf("%f ", temp_re);
      }
      printf("\n");

      
      /* Test intensity */
      dm_array_intensity(&intens_array, &array_2d_cas);
      printf("Calculated intensity from rank: \n",my_rank);
      for (i = 0; i < print_limit; i++) {
          temp_re =  intens_array.real_array[i];
          printf("%f ", temp_re);
      }
      printf("\n");
      
      /* Test complex array copying */
      dm_array_copy_complex(&copied_array,&array_2d_cas);
      printf("The copied array seen from rank %d: \n",my_rank);
      for (i = 0; i < print_limit; i++) {
          temp_re =  c_re(copied_array.complex_array,i);
          temp_im = c_im(copied_array.complex_array,i);
          printf("{%f,%f} ", temp_re,temp_im);
      }
      printf("\n");
      
      /* Test multiply complex_byte */
      dm_array_multiply_complex_byte(&array_2d_cas,&byte_array);
      printf("After multiplying complex by byte from rank %d: \n",my_rank);
      for (i = 0; i < print_limit; i++) {
          temp_re =  c_re(array_2d_cas.complex_array,i);
          temp_im = c_im(array_2d_cas.complex_array,i);
          printf("{%f,%f} ", temp_re,temp_im);
      }
      printf("\n");
      
      /* Test multiplying complex by complex */
      dm_array_multiply_complex(&array_2d_cas,&copied_array);
      printf("After multiplying complex from rank %d: \n",my_rank);
      for (i = 0; i < print_limit; i++) {
          temp_re =  c_re(array_2d_cas.complex_array,i);
          temp_im = c_im(array_2d_cas.complex_array,i);
          printf("{%f,%f} ", temp_re,temp_im);
      }
      printf("\n");
      
      /* Test subtracting complex array */
      dm_array_subtract_complex(&array_2d_cas,&copied_array);
      printf("After subtracting complex from rank %d: \n",my_rank);
      for (i = 0; i < print_limit; i++) {
          temp_re =  c_re(array_2d_cas.complex_array,i);
          temp_im = c_im(array_2d_cas.complex_array,i);
          printf("{%f,%f} ", temp_re,temp_im);
      }
      printf("\n");
      
      /* Test add complex array */
      dm_array_add_complex(&array_2d_cas,&copied_array);
      printf("After adding complex from rank %d: \n",my_rank);
      for (i = 0; i < print_limit; i++) {
          temp_re =  c_re(array_2d_cas.complex_array,i);
          temp_im = c_im(array_2d_cas.complex_array,i);
          printf("{%f,%f} ", temp_re,temp_im);
      }
      printf("\n");
  }
  
  dm_time(&ts);
  

  /* Test the fft - create a plan first */
  dm_array_fft(&array_2d_cas,p,DM_ARRAY_CREATE_FFT_PLAN | DM_ARRAY_FFT_MEASURE, 
	       my_rank);

  dm_time(&te);
  
  
  tdelta = dm_time_diff(ts,te);
  printf("Plan creation time: %f\n",tdelta);

  dm_time(&ts_total);
 
  for (i=0;i<nffts;i++) {
  
      dm_time(&ts);
  
      /* Perform forward and inverse fft */
      dm_array_fft(&array_2d_cas,p,DM_ARRAY_FORWARD_FFT, 
                   my_rank);
      
      dm_array_fft(&array_2d_cas,p,DM_ARRAY_INVERSE_FFT, 
                   my_rank);
      dm_time(&te);
      tdelta = dm_time_diff(ts,te);
      printf("Time for one Fourier transform pair: %f\n",tdelta);
      
      printf("Array after forward and inverse FT as seen from rank %d: \n",
             my_rank);
      for (j = 0; j < print_limit; j++) {
          temp_re =  c_re(array_2d_cas.complex_array,j);
          temp_im = c_im(array_2d_cas.complex_array,j);
          printf("{%f,%f} ", temp_re,temp_im);
      }
      printf("\n");
      printf("Done %d out of %d FFT cycles.\n",(i+1),nffts);
  }

  dm_time(&te_total);
  tdelta = dm_time_diff(ts_total,te_total);
  printf("Total time for %d Fourier transform pairs: %f.\n",nffts, tdelta);
  printf("Average time for 1 Fourier transform pair: %f.\n",tdelta/nffts);
  
  /* Destroy the plan */ 
  dm_array_fft(&array_2d_cas,p,DM_ARRAY_DESTROY_FFT_PLAN, 
               my_rank);
  
  /* Free the arrays */
  DM_ARRAY_COMPLEX_FREE(array_2d_cas.complex_array);
  DM_ARRAY_COMPLEX_FREE(value_to_add);

  if (!is_fftonly) {
      DM_ARRAY_COMPLEX_FREE(copied_array.complex_array);
      free(real_array.real_array);
      free(intens_array.real_array);
      free(byte_array.byte_array);
      DM_ARRAY_COMPLEX_FREE(multipl_value);
  }
  dm_exit();
  

  return(0); 
}
