#include "dm_array.h"
#include <stdio.h>
#include <math.h>
#include <stdlib.h>
 

/*------------------------------------------------------------*/
void dm_array_copy_complex(dm_array_complex_struct *ptr_cas_dest, 
                           dm_array_complex_struct *ptr_cas_src)
{
  dm_array_index_t ipix;

  if (ptr_cas_dest->npix != ptr_cas_src->npix) return;
  /* In case we have a split array we should copy real and imaginary
   * pixel-by-pixel instead of just using memcpy()
   */
  for (ipix=0; ipix<ptr_cas_dest->local_npix; ipix++) {
    c_re(ptr_cas_dest->complex_array,ipix) = 
      c_re(ptr_cas_src->complex_array,ipix);
    c_im(ptr_cas_dest->complex_array,ipix) = 
      c_im(ptr_cas_src->complex_array,ipix);
  }
#if USE_MPI
  MPI_Barrier(MPI_COMM_WORLD);
#endif /*USE_MPI*/
}

/*------------------------------------------------------------*/
void dm_array_copy_real(dm_array_real_struct *ptr_ras_dest, 
                        dm_array_real_struct *ptr_ras_src)
{
  dm_array_index_t ipix;

  if (ptr_ras_dest->npix != ptr_ras_src->npix) return;
  /* In rase we have a split array we should copy real and imaginary
   * pixel-by-pixel instead of just using memcpy()
   */
  for (ipix=0; ipix<ptr_ras_dest->local_npix; ipix++) {
    *(ptr_ras_dest->real_array+ipix) = 
        *(ptr_ras_src->real_array+ipix);
  }
#if USE_MPI
  MPI_Barrier(MPI_COMM_WORLD);
#endif /*USE_MPI*/
}

/*------------------------------------------------------------*/
void dm_array_reassign_real(dm_array_real_struct *ptr_ras_dest,
			    dm_array_real_struct *ptr_ras_src,
			    int allocated)
{
  /* free existing memory */
  if (allocated) 
    free(ptr_ras_dest->real_array);
  
  /* reassign structure values */
  ptr_ras_dest->nx = ptr_ras_src->nx;
  ptr_ras_dest->ny = ptr_ras_src->ny;
  ptr_ras_dest->nz = ptr_ras_src->nz;
  ptr_ras_dest->npix = ptr_ras_src->npix;
  ptr_ras_dest->local_npix = ptr_ras_src->local_npix;
  ptr_ras_dest->local_offset = ptr_ras_src->local_offset;

  
  /* now reassign */
  ptr_ras_dest->real_array = ptr_ras_src->real_array;
}

/*------------------------------------------------------------*/
void dm_array_crop_2d_real(dm_array_real_struct *ptr_ras,
			   int xoffset, int yoffset,
			   int my_rank, int p)
{
  int new_dim, nx, ny, new_ny, new_nx;
  int new_xcenter, new_ycenter;
  int xstart, ystart, ix, iy, i_yoffset;
  dm_array_real_struct temp_array;

  if (my_rank == 0) {
      
    nx = ptr_ras->nx;
    ny = ptr_ras->ny;
    
    /* Check if square and if already centered */
    if ((xoffset != 0) || (yoffset != 0) || (nx != ny)) {
#ifdef DIST_FFT
      
#else
      new_nx = nx - 2*abs(xoffset);
      new_ny = ny - 2*abs(yoffset);
      new_xcenter = nx/2 - xoffset;
      new_ycenter = ny/2 - yoffset;
      new_dim = (new_nx >= new_ny) ? new_ny : new_nx;
      xstart = (new_nx >= new_ny) ? (new_xcenter - new_ny/2) : 
	(new_xcenter - new_nx/2); 
      ystart = (new_nx <= new_ny) ? (new_ycenter - new_nx/2) : 
	(new_ycenter - new_ny/2); 
#endif /* DIST_FFT */
      
      /* Now allocate the cropped arrays */
      temp_array.nx = new_dim;
      temp_array.ny = new_dim;
      temp_array.nz = 1;
      temp_array.npix = (dm_array_index_t)new_dim*(dm_array_index_t)new_dim;
      DM_ARRAY_REAL_STRUCT_INIT((&temp_array),temp_array.npix,p)
 
	/* Transfer */
	for (iy=0;iy<new_dim;iy++) {
	  i_yoffset = ystart+ny*iy;
	  for (ix=0;ix<new_dim;ix++) {
	    *(temp_array.real_array + ix + new_dim*iy) = 
	      *(ptr_ras->real_array + (xstart+ix + i_yoffset));
	  }
	}
      
      /* free old arrays and transfer memory */
      dm_array_reassign_real(ptr_ras,&temp_array,1);
    } 
  } /* endif(my_rank == 0) */
}
/*------------------------------------------------------------*/
void dm_array_transfer_magnitudes(dm_array_complex_struct *ptr_cas_dest, 
                                  dm_array_real_struct *ptr_ras_mags,
                                  dm_array_real_struct *ptr_ras_errors,
				  int zero_if_not_known)
{
  dm_array_index_t ipix;
  dm_array_real this_old_mag, this_re, this_im, this_new_mag;
  dm_array_real this_error;
  int with_errors = 1;
  
  if (ptr_cas_dest->npix != ptr_ras_mags->npix) return;

  if (ptr_ras_errors == NULL) {
      with_errors = 0;
  } else { 
      if (ptr_cas_dest->npix != ptr_ras_errors->npix) return;      
  }
  
  for (ipix=0; ipix<ptr_cas_dest->local_npix; ipix++) {
      /* Only if we actually measured the other magnitudes */
      if (*(ptr_ras_mags->real_array+ipix)) {
          this_re = c_re(ptr_cas_dest->complex_array,ipix);
          this_im = c_im(ptr_cas_dest->complex_array,ipix);
          this_old_mag = sqrt(this_re*this_re + this_im*this_im);
          this_new_mag = *(ptr_ras_mags->real_array+ipix);
          if (with_errors) {
              this_error = *(ptr_ras_errors->real_array+ipix);
              
              if (this_old_mag > (this_new_mag+this_error)) {
                  this_new_mag += this_error;
              } else if (this_old_mag < (this_new_mag-this_error)) {
                  this_new_mag -= this_error;
              } else {
                  this_new_mag = this_old_mag;
              }
          }        
	  /* Unlikely that old_mag will be 0 but just in case */
	  if (this_old_mag) {
	    c_re(ptr_cas_dest->complex_array,ipix) *= 
              this_new_mag/this_old_mag;
	    c_im(ptr_cas_dest->complex_array,ipix) *= 
              this_new_mag/this_old_mag;
	  } else {
	    c_re(ptr_cas_dest->complex_array,ipix) += this_new_mag;
	  }
      } else {
	if (zero_if_not_known) {
	  c_re(ptr_cas_dest->complex_array,ipix) = 0.0; 
	  c_im(ptr_cas_dest->complex_array,ipix) = 0.0;
	}
      }
  }
  
#if USE_MPI
  MPI_Barrier(MPI_COMM_WORLD);
#endif
}

/*------------------------------------------------------------*/
void dm_array_subtract_real(dm_array_real_struct *ptr_ras_diff,
                            dm_array_real_struct *ptr_ras) 
{
  dm_array_index_t ipix;

  if (ptr_ras_diff->npix != ptr_ras->npix) return;

  for (ipix=0; ipix<ptr_ras_diff->local_npix; ipix++) {
    *(ptr_ras_diff->real_array+ipix) -=
      *(ptr_ras->real_array+ipix);
  }
#if USE_MPI
  MPI_Barrier(MPI_COMM_WORLD);
#endif
}

/*------------------------------------------------------------*/
void dm_array_add_real(dm_array_real_struct *ptr_ras_sum,
                       dm_array_real_struct *ptr_ras) 
{
  dm_array_index_t ipix;

  if (ptr_ras_sum->npix != ptr_ras->npix) return;

  for (ipix=0; ipix<ptr_ras_sum->local_npix; ipix++) {
    *(ptr_ras_sum->real_array+ipix) +=
      *(ptr_ras->real_array+ipix);
  }
#if USE_MPI
  MPI_Barrier(MPI_COMM_WORLD);
#endif /* USE_MPI */
}

/*------------------------------------------------------------*/
void dm_array_add_real_scalar(dm_array_complex_struct *ptr_cas,
			      dm_array_real scalar_value) 
{
  dm_array_index_t ipix;
  
  for (ipix=0; ipix<ptr_cas->local_npix; ipix++) {
    c_re(ptr_cas->complex_array,ipix) += scalar_value;
  }
#if USE_MPI
  MPI_Barrier(MPI_COMM_WORLD);
#endif /* USE_MPI */
}

/*------------------------------------------------------------*/
void dm_array_subtract_complex(dm_array_complex_struct *ptr_cas_diff,
                               dm_array_complex_struct *ptr_cas) 
{
  dm_array_index_t ipix;
  
  if (ptr_cas_diff->npix != ptr_cas->npix) return;

  for (ipix=0; ipix<ptr_cas_diff->local_npix; ipix++) {
    c_re(ptr_cas_diff->complex_array,ipix) -=
      c_re(ptr_cas->complex_array,ipix);
    c_im(ptr_cas_diff->complex_array,ipix) -=
      c_im(ptr_cas->complex_array,ipix);
  }

#if USE_MPI
  MPI_Barrier(MPI_COMM_WORLD);
#endif /* USE_MPI */
}

/*------------------------------------------------------------*/
void dm_array_add_complex(dm_array_complex_struct *ptr_cas_sum,
                          dm_array_complex_struct *ptr_cas) 
{
  dm_array_index_t ipix;

  if (ptr_cas_sum->npix != ptr_cas->npix) return;

  for (ipix=0; ipix<ptr_cas_sum->local_npix; ipix++) {
    c_re(ptr_cas_sum->complex_array,ipix) +=
      c_re(ptr_cas->complex_array,ipix);
    c_im(ptr_cas_sum->complex_array,ipix) +=
      c_im(ptr_cas->complex_array,ipix);
  }

#if USE_MPI
  MPI_Barrier(MPI_COMM_WORLD);
#endif /* USE_MPI */
}

/*------------------------------------------------------------*/
void dm_array_add_complex_scalar(dm_array_complex_struct *ptr_cas, 
				 dm_array_complex *ptr_scalar_value) 
{
  dm_array_index_t ipix;
  dm_array_real sc_re, sc_im;
  
  sc_re = c_re(ptr_scalar_value,0);
  sc_im = c_im(ptr_scalar_value,0);

  for (ipix=0; ipix<ptr_cas->local_npix; ipix++) {
    c_re(ptr_cas->complex_array,ipix) += sc_re;
    c_im(ptr_cas->complex_array,ipix) += sc_im;
  }

#if USE_MPI
  MPI_Barrier(MPI_COMM_WORLD);
#endif /* USE_MPI */
}

/*------------------------------------------------------------*/
void dm_array_multiply_real_scalar(dm_array_complex_struct *ptr_cas, 
				   dm_array_real scalar_value) 
{
  dm_array_index_t ipix;
  
  for (ipix=0; ipix<ptr_cas->local_npix; ipix++) {
    c_re(ptr_cas->complex_array,ipix) *= scalar_value;
    c_im(ptr_cas->complex_array,ipix) *= scalar_value;
  }  

#if USE_MPI
  MPI_Barrier(MPI_COMM_WORLD);
#endif /* USE_MPI */
}

/*------------------------------------------------------------*/
void dm_array_multiply_complex_scalar(dm_array_complex_struct *ptr_cas, 
				      dm_array_complex *ptr_scalar_value) 
{
  dm_array_index_t ipix;
  dm_array_real sc_re, sc_im, pix_re, pix_im, result_re, result_im;

  sc_re = c_re(ptr_scalar_value,0);
  sc_im = c_im(ptr_scalar_value,0);
  
  for (ipix=0; ipix<ptr_cas->local_npix; ipix++) {
    pix_re = c_re(ptr_cas->complex_array,ipix);
    pix_im = c_im(ptr_cas->complex_array,ipix);
    result_re = pix_re*sc_re-pix_im*sc_im;
    result_im = pix_re*sc_im+pix_im*sc_re;
    c_re(ptr_cas->complex_array,ipix) = result_re; 
    c_im(ptr_cas->complex_array,ipix) = result_im;
  }

#if USE_MPI
  MPI_Barrier(MPI_COMM_WORLD);
#endif /* USE_MPI */
}

/*------------------------------------------------------------*/
void dm_array_realpart(dm_array_real_struct *ptr_ras,
		       dm_array_complex_struct *ptr_cas)
{
  dm_array_index_t ipix;
  dm_array_complex *local_array_cas;
  dm_array_real *local_array_ras;
  
  if ((ptr_ras->npix) != (ptr_cas->npix)) return;
  
  for (ipix=0; ipix<ptr_ras->local_npix; ipix++) {
    *(ptr_ras->real_array+ipix) = c_re(ptr_cas->complex_array,ipix);
  }

#if USE_MPI
  MPI_Barrier(MPI_COMM_WORLD);
#endif /* USE_MPI */
}

/*------------------------------------------------------------*/
void dm_array_imaginarypart(dm_array_real_struct *ptr_ras,
			    dm_array_complex_struct *ptr_cas)
{
  dm_array_index_t ipix;
  
  if ((ptr_ras->npix) != (ptr_cas->npix)) return;
  
  for (ipix=0; ipix<ptr_ras->local_npix; ipix++) {
    *(ptr_ras->real_array+ipix) = c_im(ptr_cas->complex_array,ipix);
  }

#if USE_MPI
  MPI_Barrier(MPI_COMM_WORLD);
#endif /* USE_MPI */
}

/*------------------------------------------------------------*/
void dm_array_square_sum_complex(dm_array_complex_struct *ptr_cas,
				 dm_array_complex_struct *ptr_c_cas,
				 dm_array_complex *ptr_complex_sum)
{
  dm_array_real local_re,local_im,global_re,global_im;
  dm_array_index_t ipix;
  dm_array_real temp_re, temp_im, temp_c_re,temp_c_im;

  if (ptr_c_cas != NULL) {
    if ((ptr_cas->local_npix) != 
	(ptr_c_cas->local_npix)) return;
  }

  if (ptr_c_cas == NULL) {
    for (ipix=0;ipix<ptr_cas->local_npix;ipix++) {
      temp_re = c_re(ptr_cas->complex_array,ipix);
      temp_im = c_im(ptr_cas->complex_array,ipix);

      local_re += temp_re*temp_re - temp_im*temp_im;
      local_im += 2*temp_re*temp_im;
    }
#if USE_MPI
    MPI_Barrier(MPI_COMM_WORLD);
    MPI_Reduce(&local_re, &global_re, 1, 
	       MPI_ARRAY_REAL, MPI_SUM, 0, MPI_COMM_WORLD);
    MPI_Reduce(&local_im, &global_im, 1, 
	       MPI_ARRAY_REAL, MPI_SUM, 0, MPI_COMM_WORLD);
    /* Now report back to other processes */
    MPI_Bcast(&global_re,1,MPI_ARRAY_REAL,0,MPI_COMM_WORLD);
    MPI_Bcast(&global_im,1,MPI_ARRAY_REAL,0,MPI_COMM_WORLD);
#else 
    global_re = local_re;
    global_im = local_im;
#endif /* USE_MPI */    
  } else {
    /* We calculate the mixed sum */
    for (ipix=0;ipix<ptr_cas->local_npix;ipix++) {
      temp_re = c_re(ptr_cas->complex_array,ipix);
      temp_im = c_im(ptr_cas->complex_array,ipix);

      temp_c_re = c_re(ptr_c_cas->complex_array,ipix);
      temp_c_im = c_im(ptr_c_cas->complex_array,ipix);
      
      local_re += temp_re*temp_c_re + temp_im*temp_c_im;
      local_im += temp_im*temp_c_re - temp_re*temp_c_im;
    }
#if USE_MPI
    MPI_Barrier(MPI_COMM_WORLD);
    MPI_Reduce(&local_re, &global_re, 1, 
	       MPI_ARRAY_REAL, MPI_SUM, 0, MPI_COMM_WORLD);
    MPI_Reduce(&local_im, &global_im, 1, 
	       MPI_ARRAY_REAL, MPI_SUM, 0, MPI_COMM_WORLD);
    /* Now report back to other processes */
    MPI_Bcast(&global_re,1,MPI_ARRAY_REAL,0,MPI_COMM_WORLD);
    MPI_Bcast(&global_im,1,MPI_ARRAY_REAL,0,MPI_COMM_WORLD);
#else 
    global_re = local_re;
    global_im = local_im;
#endif /* USE_MPI */    

  } /* endif(ptr_c_cas == NULL) */

  /* load values into complex scalar */
  c_re(ptr_complex_sum,0) = global_re;
  c_im(ptr_complex_sum,0) = global_im;
}


/*------------------------------------------------------------*/
void dm_array_magnitude_complex(dm_array_real_struct *ptr_ras,
                                dm_array_complex_struct *ptr_cas)
{
  dm_array_index_t ipix;
  double temp_re, temp_im;
  
  if ((ptr_ras->npix) != (ptr_cas->npix)) return;
  
  for (ipix=0; ipix<ptr_ras->local_npix; ipix++) {
    temp_re = (double)c_re(ptr_cas->complex_array,ipix);
    temp_im = (double)c_im(ptr_cas->complex_array,ipix);
    *(ptr_ras->real_array+ipix) = 
      (dm_array_real)sqrt(temp_re*temp_re+temp_im*temp_im);
  }

#if USE_MPI
  MPI_Barrier(MPI_COMM_WORLD);
#endif /* USE_MPI */
}


/*------------------------------------------------------------*/
void dm_array_magnitude_real(dm_array_real_struct *ptr_ras_mag,
                             dm_array_real_struct *ptr_ras,
                             int is_intensities)
{
  dm_array_index_t ipix;
  dm_array_real temp_mag;
 
  if ((ptr_ras_mag->npix) != (ptr_ras->npix)) return;
  
  for (ipix=0; ipix<ptr_ras->local_npix; ipix++) {
#if defined (DM_ARRAY_DOUBLE)
      temp_mag = fabs(*(ptr_ras->real_array+ipix));
#else
      temp_mag = fabsf(*(ptr_ras->real_array+ipix));
#endif
      if (is_intensities) {
          *(ptr_ras_mag->real_array+ipix) =
              sqrt(temp_mag);
      } else {
          *(ptr_ras_mag->real_array+ipix) =
              temp_mag;
      }
  }

#if USE_MPI
  MPI_Barrier(MPI_COMM_WORLD);
#endif /* USE_MPI */
}

/*------------------------------------------------------------*/
void dm_array_phase(dm_array_real_struct *ptr_ras,
		    dm_array_complex_struct *ptr_cas)
{
  dm_array_index_t ipix;
  dm_array_real temp_re, temp_im;
  
  if ((ptr_ras->npix) != (ptr_cas->npix)) return;
  
  for (ipix=0; ipix<ptr_ras->local_npix; ipix++) {
    temp_re = c_re(ptr_cas->complex_array,ipix);
    temp_im = c_im(ptr_cas->complex_array,ipix);
    *(ptr_ras->real_array+ipix) = atan2(temp_im,temp_re);
  }

#if USE_MPI
  MPI_Barrier(MPI_COMM_WORLD);
#endif /* USE_MPI */
}

/*------------------------------------------------------------*/
dm_array_real dm_array_global_phase(dm_array_complex_struct *ptr_cas)
{
    dm_array_index_t ipix;
    dm_array_real temp_re, temp_im;
    dm_array_real local_phase, global_phase;

    local_phase = 0.0;
    global_phase = 0.0;
    for (ipix=0; ipix<ptr_cas->local_npix; ipix++) {
        temp_re = c_re(ptr_cas->complex_array,ipix);
        temp_im = c_im(ptr_cas->complex_array,ipix);
        local_phase +=
            (dm_array_real)(atan2((double)temp_im,(double)temp_re)/
                            (double)ptr_cas->npix);
    }
    
#if USE_MPI
    MPI_Barrier(MPI_COMM_WORLD);
    MPI_Reduce(&local_phase, &global_phase, 1, 
               MPI_ARRAY_REAL, MPI_SUM, 0, MPI_COMM_WORLD);
    
    /* Now broadcast to all processes */
    MPI_Bcast(&global_phase,1,MPI_ARRAY_REAL,0,MPI_COMM_WORLD);

#else
    global_phase = local_phase;
#endif /* USE_MPI */

    return(global_phase);
}

/*------------------------------------------------------------*/
/** This routine puts the intensity (square) of complex_array 
    into real_array */
void dm_array_intensity(dm_array_real_struct *ptr_ras,
			dm_array_complex_struct *ptr_cas)
{
  dm_array_index_t ipix;
  dm_array_real temp_re, temp_im;
  
  if ((ptr_ras->npix) != (ptr_cas->npix)) return;
  
  for (ipix=0; ipix<ptr_ras->local_npix; ipix++) {
    temp_re = c_re(ptr_cas->complex_array,ipix);
    temp_im = c_im(ptr_cas->complex_array,ipix);
    *(ptr_ras->real_array+ipix) = temp_re*temp_re+temp_im*temp_im;
  }

#if USE_MPI
  MPI_Barrier(MPI_COMM_WORLD);
#endif /* USE_MPI */
}

/*------------------------------------------------------------*/
void dm_array_zero_complex(dm_array_complex_struct *ptr_cas)
{
  dm_array_index_t ipix;
  
  for (ipix=0; ipix<ptr_cas->local_npix; ipix++) {
      c_re(ptr_cas->complex_array,ipix) = (dm_array_real)0.;
      c_im(ptr_cas->complex_array,ipix) = (dm_array_real)0.;
  }

#if USE_MPI
  MPI_Barrier(MPI_COMM_WORLD);
#endif /* USE_MPI */
}

/*------------------------------------------------------------*/
void dm_array_zero_real(dm_array_real_struct *ptr_ras)
{
  dm_array_index_t ipix;
  
  for (ipix=0; ipix<ptr_ras->local_npix; ipix++) {
      *(ptr_ras->real_array+ipix) = (dm_array_real)0.;
  }

#if USE_MPI
  MPI_Barrier(MPI_COMM_WORLD);
#endif /* USE_MPI */
}

/*------------------------------------------------------------*/
dm_array_real dm_array_total_power_complex(dm_array_complex_struct *ptr_cas,
					   dm_array_byte_struct *ptr_indices,
					   int inverse)
{
  dm_array_index_t ipix;
  dm_array_real total_power, local_power; /* use lots of precision
                                             for this even if
                                             dm_array_real is a float */
  dm_array_real this_re, this_im;
  
  if (ptr_indices != NULL) {
    if (ptr_indices->local_npix != ptr_cas->local_npix) return;
  }

  local_power = 0.;
  total_power = 0.;
  for (ipix=0; ipix<ptr_cas->local_npix; ipix++) {
    if (ptr_indices != NULL) {
      if (inverse) {
	if (*(ptr_indices->byte_array + ipix) == 0) {
	  this_re = c_re(ptr_cas->complex_array,ipix);
	  this_im = c_im(ptr_cas->complex_array,ipix);
	  local_power +=  this_re*this_re+this_im*this_im;
          //printf("local_power += %f\n",local_power);
	}	
      } else {
	if (*(ptr_indices->byte_array + ipix) == 1) {
	  this_re = c_re(ptr_cas->complex_array,ipix);
	  this_im = c_im(ptr_cas->complex_array,ipix);
	  local_power +=  this_re*this_re+this_im*this_im;
          //printf("local_power += %f\n",local_power);
	}
      }
    } else {
      this_re = c_re(ptr_cas->complex_array,ipix);
      this_im = c_im(ptr_cas->complex_array,ipix);
      local_power +=  this_re*this_re+this_im*this_im;
    }
  }

#if USE_MPI
  MPI_Barrier(MPI_COMM_WORLD);
  MPI_Reduce(&local_power, &total_power, 1, 
	     MPI_ARRAY_REAL, MPI_SUM, 0, MPI_COMM_WORLD);

  /* Now broadcast to all processes */
  MPI_Bcast(&total_power,1,MPI_ARRAY_REAL,0,MPI_COMM_WORLD);
  
#else 
  total_power = local_power;
#endif /* USE_MPI */

  return((dm_array_real)total_power);
}

/*------------------------------------------------------------*/
dm_array_real dm_array_total_power_real(dm_array_real_struct *ptr_ras,
                                        int is_intensities)
{
  dm_array_index_t ipix;
  dm_array_real total_power, local_power; /* use lots of precision for
                                             this even if
                                             dm_array_real is a float */
  dm_array_real this_re, this_im;
  
  local_power = 0.;
  total_power = 0.;
  for (ipix=0; ipix<ptr_ras->local_npix; ipix++) {
      if (is_intensities) {
          local_power +=  *(ptr_ras->real_array+ipix);
      } else {
          local_power +=  (*(ptr_ras->real_array+ipix)*
                           *(ptr_ras->real_array+ipix));
      }
  }

#if USE_MPI
  MPI_Barrier(MPI_COMM_WORLD);
  MPI_Reduce(&local_power, &total_power, 1, 
	     MPI_ARRAY_REAL, MPI_SUM, 0, MPI_COMM_WORLD);

  /* Now broadcast to all processes */
  MPI_Bcast(&total_power,1,MPI_ARRAY_REAL,0,MPI_COMM_WORLD);
#else 
  total_power = local_power;
#endif /* USE_MPI */

  return((dm_array_real)total_power);
}

/*------------------------------------------------------------*/
dm_array_real dm_array_max_real(dm_array_real_struct *ptr_ras,
                                int p)
{
    dm_array_index_t ipix;
    dm_array_real current_max;
#if USE_MPI
    dm_array_real local_maxs[p];
    
    current_max = *(ptr_ras->real_array);
    
    for (ipix=1;ipix<ptr_ras->local_npix;ipix++) {
        if (*(ptr_ras->real_array+ipix) > current_max) {
            current_max = *(ptr_ras->real_array+ipix);
        }
    }
    
    /* Tell each process about all local maxima and then have them
       determine the global maximum independently.
     */
    MPI_Allgather(&current_max,1,MPI_ARRAY_REAL,
                  local_maxs,1,MPI_ARRAY_REAL,MPI_COMM_WORLD);

    current_max = local_maxs[0];
    for (ipix=0;ipix<p;ipix++) {
        if (local_maxs[ipix] > current_max) {
            current_max = local_maxs[ipix];
        }
    }

    MPI_Barrier(MPI_COMM_WORLD);

#else /* This is for non-MPI users */
    current_max = *(ptr_ras->real_array + 0);
    
    for (ipix=1;ipix<ptr_ras->npix;ipix++) {
        if (*(ptr_ras->real_array+ipix) > current_max) {
            current_max = *(ptr_ras->real_array+ipix);
        }
    }    
#endif /* USE_MPI */

    return((dm_array_real)current_max);

}

/*------------------------------------------------------------*/
dm_array_real dm_array_min_real(dm_array_real_struct *ptr_ras,
                                int p)
{
    dm_array_index_t ipix;
    dm_array_real current_min;
#if USE_MPI
    dm_array_real local_mins[p];
    
    current_min = *(ptr_ras->real_array);
    
    for (ipix=1;ipix<ptr_ras->local_npix;ipix++) {
        if (*(ptr_ras->real_array+ipix) < current_min) {
            current_min = *(ptr_ras->real_array+ipix);
        }
    }
    
    /* Tell each process about all local minima and then have them
       determine the global minimum independently.
     */
    MPI_Allgather(&current_min,1,MPI_ARRAY_REAL,
                  local_mins,1,MPI_ARRAY_REAL,MPI_COMM_WORLD);

    current_min = local_mins[0];
    for (ipix=0;ipix<p;ipix++) {
        if (local_mins[ipix] < current_min) {
            current_min = local_mins[ipix];
        }
    }

    MPI_Barrier(MPI_COMM_WORLD);

#else /* This is for non-MPI users */
    current_min = *(ptr_ras->real_array + 0);
    
    for (ipix=1;ipix<ptr_ras->npix;ipix++) {
        if (*(ptr_ras->real_array+ipix) < current_min) {
            current_min = *(ptr_ras->real_array+ipix);
        }
    }    
#endif /* USE_MPI */

    return((dm_array_real)current_min);

}

/*------------------------------------------------------------*/
void dm_array_rand(dm_array_complex_struct *ptr_cas,
		   int imaginary_too)
{
    dm_array_index_t ipix;
    time_t seed = time(NULL);

    for (ipix=0;ipix<ptr_cas->local_npix;ipix++) {
      c_re(ptr_cas->complex_array,ipix) = dm_rand(&seed);
      if (imaginary_too) {
	c_im(ptr_cas->complex_array,ipix) = dm_rand(&seed);
      }
    }

#if USE_MPI
    MPI_Barrier(MPI_COMM_WORLD);
#endif /* USE_MPI */
}

/*------------------------------------------------------------*/
void dm_array_load_gaussian(dm_array_complex_struct *ptr_cas,
			    dm_array_real sigma_x,
			    dm_array_real sigma_y,
			    dm_array_real sigma_z,
			    int inverse,
			    int fft_centered,
                            int p,
                            int my_rank)
{
  dm_array_index_t ix, iy, iz, yoffset, zoffset, offset, local_n;
  dm_array_index_t local_nx, local_ny, local_nz;
  dm_array_real *xarr, *yarr, *zarr;
  dm_array_real inverse_sigma_x, inverse_sigma_y, inverse_sigma_z;
  dm_array_real this_x, this_y, this_z;
  dm_array_real half_nx, half_ny, half_nz;

#if USE_MPI
  local_n = ptr_cas->npix/p;
  if (ptr_cas->ny == 1) {
      local_nx = ptr_cas->nx/p;
      local_ny = 1;
      local_nz = 1;
  } else if (ptr_cas->nz == 1) {
      local_nx = ptr_cas->nx;
      local_ny = ptr_cas->ny/p;
      local_nz = 1;
  } else {
      local_nx = ptr_cas->nx;
      local_ny = ptr_cas->ny;
      local_nz = ptr_cas->nz/p;
  }
#else
  local_n = ptr_cas->npix;
  local_nx = ptr_cas->nx;
  local_ny = ptr_cas->ny;
  local_nz = ptr_cas->nz;
#endif /* USE_MPI */
  
  inverse_sigma_x = 0.;
  inverse_sigma_y = 0.;
  inverse_sigma_z = 0.;
  if (sigma_x != 0.) inverse_sigma_x = 0.5/(sigma_x*sigma_x);
  if (sigma_y != 0.) inverse_sigma_y = 0.5/(sigma_y*sigma_y);
  if (sigma_z != 0.) inverse_sigma_z = 0.5/(sigma_z*sigma_z);

  /* Pre-calculate arrays for each dimension.  Otherwise we'd
     calculate the X contribution (ny*nz) times, and the Y
     contribution (nz) times. Take MPI into account by using local_*.
  */
  xarr = (dm_array_real *)malloc(local_nx*sizeof(dm_array_real));
  yarr = (dm_array_real *)malloc(local_ny*sizeof(dm_array_real));
  zarr = (dm_array_real *)malloc(local_nz*sizeof(dm_array_real));
  
  if (local_ny == 1) {
      if (local_nx > 1) {
	/* Center of Gaussian is in the absolute center of array 
	 * if data-centered, otherwise it is at f(0,0,0)
	 */
	for (ix=0; ix<local_nx; ix++) {
	  if (fft_centered == 1) {
	    this_x = (my_rank*local_nx+ix);
	    (this_x<ptr_cas->nx/2)?(this_x=this_x):(this_x=this_x-ptr_cas->nx);
	  } else {
	    this_x = (my_rank*local_nx+ix) - ptr_cas->nx/2;
	  }
	  *(xarr+ix) = exp(-this_x*this_x*inverse_sigma_x);
	}
      } else {
	*(xarr+0) = 1.;
      }
      *(yarr+0) = 1.;
      *(zarr+0) = 1.;
  } else if (local_nz == 1) {
      if (local_nx > 1) {
	/* Center of Gaussian is in the absolute center of array 
	 * if data-centered, otherwise it is at f(0,0,0)
	 */
	for (ix=0; ix<local_nx; ix++) {
	  if (fft_centered == 1) {
	    this_x = ix;
	    (this_x<ptr_cas->nx/2)?(this_x=this_x):(this_x=this_x-ptr_cas->nx);
	  } else {
	    this_x = ix - ptr_cas->nx/2;
	  }
	  *(xarr+ix) = exp(-this_x*this_x*inverse_sigma_x);
	}
      } else {
          *(xarr+0) = 1.;
      }
      if (local_ny > 1) {
	/* Center of Gaussian is in the absolute center of array 
	 * if data-centered, otherwise it is at f(0,0,0)
	 */
	for (iy=0; iy<local_ny; iy++) {
	  if (fft_centered == 1) {
	    this_y = (my_rank*local_ny+iy);
	    (this_y<ptr_cas->ny/2)?(this_y=this_y):(this_y=this_y-ptr_cas->ny);
	  } else {
	    this_y = (my_rank*local_ny+iy) - ptr_cas->ny/2;
	  }
	  *(yarr+iy) = exp(-this_y*this_y*inverse_sigma_y);
	}
      } else {
	*(yarr+0) = 1.;
      }
      *(zarr+0) = 1.;
  } else {
    if (local_nx > 1) {
      /* Center of Gaussian is in the absolute center of array 
       * if data-centered, otherwise it is at f(0,0,0)
       */
      for (ix=0; ix<local_nx; ix++) {
	if (fft_centered == 1) {
	  this_x = ix;
	  (this_x<ptr_cas->nx/2)?(this_x=this_x):(this_x=this_x-ptr_cas->nx);
	} else {
	  this_x = ix - ptr_cas->nx/2;
	}
	*(xarr+ix) = exp(-this_x*this_x*inverse_sigma_x);
      }
    } else {
      *(xarr+0) = 1.;
    }
    if (local_ny > 1) {
      /* Center of Gaussian is in the absolute center of array 
       * if data-centered, otherwise it is at f(0,0,0)
       */
      for (iy=0; iy<local_ny; iy++) {
	if (fft_centered == 1) {
	  this_y = iy;
	  (this_y<ptr_cas->ny/2)?(this_y=this_y):(this_y=this_y-ptr_cas->ny);
	} else {
	  this_y = iy - ptr_cas->ny/2;
	}
	*(yarr+iy) = exp(-this_y*this_y*inverse_sigma_y);
      }
    } else {
      *(yarr+0) = 1.;
    }
    if (local_nz > 1) {
      /* Center of Gaussian is in the absolute center of array 
       * if data-centered, otherwise it is at f(0,0,0)
       */
      for (iz=0; iz<local_nz; iz++) {
	if (fft_centered == 1) {
	  this_z = (my_rank*local_nz+iz);
	  (this_z<ptr_cas->nz/2)?(this_z=this_z):(this_z=this_z-ptr_cas->nz);
	} else {
	  this_z = (my_rank*local_nz+iz) - ptr_cas->nz/2;
	}
	*(zarr+iz) = exp(-this_z*this_z*inverse_sigma_z);
      }
    } else {
      *(zarr+0) = 1.;
    }
  }
  
  for (iz=0; iz<local_nz; iz++) {
    zoffset = local_nx*local_ny*iz;
    this_z = *(zarr+iz);
    for (iy=0; iy<local_ny; iy++) {
      yoffset = local_nx*iy+zoffset;
      this_y = *(yarr+iy);
      for (ix=0; ix<local_nx; ix++) {
	offset = ix+yoffset;
        if (inverse) {
            c_re(ptr_cas->complex_array,offset) =
                1-(*(xarr+ix))*this_y*this_z;
        } else {
            c_re(ptr_cas->complex_array,offset) =
                (*(xarr+ix))*this_y*this_z;
        }
        c_im(ptr_cas->complex_array,offset) = 0.;
      }
    }
  }

  free(xarr);
  free(yarr);
  free(zarr);
#if USE_MPI
  MPI_Barrier(MPI_COMM_WORLD);
#endif
  
}

/*------------------------------------------------------------*/
void dm_array_multiply_complex(dm_array_complex_struct *ptr_cas_one,
                               dm_array_complex_struct *ptr_cas_two)
{
  dm_array_index_t ipix;
  dm_array_real one_im, one_re, two_im, two_re, result_re, result_im;
  
  if (ptr_cas_one->npix != ptr_cas_two->npix) return;
  /* In case we have a split array we should copy real and imaginary
   * pixel-by-pixel instead of just using memcpy()
   */
  for (ipix=0; ipix<ptr_cas_one->local_npix; ipix++) {
      one_re = c_re(ptr_cas_one->complex_array,ipix);
      one_im = c_im(ptr_cas_one->complex_array,ipix);
      two_re = c_re(ptr_cas_two->complex_array,ipix);
      two_im = c_im(ptr_cas_two->complex_array,ipix);

      result_re = one_re*two_re - one_im*two_im;
      result_im = one_im*two_re + one_re*two_im                    ;
      c_re(ptr_cas_one->complex_array,ipix) = result_re;
      c_im(ptr_cas_one->complex_array,ipix) = result_im;
  }

#if USE_MPI
  MPI_Barrier(MPI_COMM_WORLD);
#endif /*USE_MPI*/
}

/*------------------------------------------------------------*/
void dm_array_multiply_complex_byte(dm_array_complex_struct *ptr_cas,
                                    dm_array_byte_struct *ptr_bas)
{
  dm_array_index_t ipix;

  if (ptr_cas->npix != ptr_bas->npix) return;
  /* In case we have a split array we should copy real and imaginary
   * pixel-by-pixel instead of just using memcpy()
   */
  for (ipix=0; ipix<ptr_cas->local_npix; ipix++) {
    c_re(ptr_cas->complex_array,ipix) *=
        *(ptr_bas->byte_array+ipix);
    c_im(ptr_cas->complex_array,ipix) *= 
        *(ptr_bas->byte_array+ipix);
  }

#if USE_MPI
  MPI_Barrier(MPI_COMM_WORLD);
#endif /*USE_MPI*/
}
  
/*------------------------------------------------------------*/
void dm_array_fft(dm_array_complex_struct *ptr_cas,
		  int p,
		  int fft_options,
		  int my_rank)
{
  dm_array_real norm_factor = 1./sqrt((dm_array_real)(ptr_cas->nx)*
				      (dm_array_real)(ptr_cas->ny)*
				      (dm_array_real)(ptr_cas->nz));
  dm_fft_plan local_forward_plan, local_inverse_plan;
  /* Define any FFT-routine-dependent variables */
#if (defined(__APPLE__) && defined(DIST_FFT))
  dm_time_t ts, te;
  double tdelta;
  dist_fft_flags forward_flags, inverse_flags;
  dist_fft_dimension forward_dimension, forward_start;
  dist_fft_dimension inverse_dimension, inverse_start;
  size_t forward_storage_size, inverse_storage_size;
  MPI_Status status;
  dm_fft_storage workspace=NULL;
  int ic;
  int local_size = (int) ptr_cas->npix/p;
#else /* Things we need for fftw */
  unsigned fftw_flags;
  dm_array_complex_struct local_copy;
  dm_array_complex_struct data_shift;
  int ix, iy, iz, index;
  int jx, jy, jz, index_shift;
#endif
    
  /* Are we being asked to destroy a plan? */
  if ((fft_options & DM_ARRAY_DESTROY_FFT_PLAN) ==
      DM_ARRAY_DESTROY_FFT_PLAN) {
/*#ifdef DEBUG*/
    printf("    destroying an FFT plan\n");
/*#endif*/
#if (defined(__APPLE__) && defined(DIST_FFT))
    /* dist_fft destroy plan code */
    dist_fft_destroy_plan(ptr_cas->ptr_forward_plan);
    dist_fft_destroy_plan(ptr_cas->ptr_inverse_plan);
    MPI_Barrier(MPI_COMM_WORLD);
#else
    /* FFTW3 destroy plan code */
#if defined(DM_ARRAY_DOUBLE)
    printf("destroying plans\n");
    fftw_destroy_plan(ptr_cas->ptr_forward_plan);
    fftw_destroy_plan(ptr_cas->ptr_inverse_plan);
#else
    printf("destroying forward plan\n");
    fftwf_destroy_plan(ptr_cas->ptr_forward_plan);
    printf("destroying inverse plan\n");
    fftwf_destroy_plan(ptr_cas->ptr_inverse_plan);
    printf("destroyed plans\n");
#endif
#endif
  }
  
  /* Are we being asked to create a plan? */
  if ((fft_options & DM_ARRAY_CREATE_FFT_PLAN) ==
      DM_ARRAY_CREATE_FFT_PLAN) {
#ifdef DEBUG
    printf("    creating an FFT plan\n");
#endif
#if (defined(__APPLE__) && defined(DIST_FFT))
    /* This is the case of an Apple machine with dist_fft.
       The flags appear to be DIST_FFT_FORWARD, DIST_FFT_INVERSE,
       DIST_FFT_ROW_INPUT, DIST_FFT_ROW_OUTPUT, DIST_FFT_COLUMN_INPUT,
       DIST_FFT_COLUMN_OUTPUT, and DIST_FFT_VERBOSE
    */
    
    forward_flags = DIST_FFT_COLUMN_INPUT | DIST_FFT_COLUMN_OUTPUT;
    inverse_flags = DIST_FFT_COLUMN_INPUT | DIST_FFT_COLUMN_OUTPUT;

    
    /* Check the dimensionality of the input array. 
     * Then create the plans.
     */
    if (((ptr_cas->ny) == 1) && ((ptr_cas->nz) == 1)) {
      local_forward_plan = 
	dist_fft_create_plan(DIST_FFT_1D_TYPE,
			     (dist_fft_dimension)(ptr_cas->nx),
			     DIST_FFT_FORWARD,
			     forward_flags, MPI_COMM_WORLD);
      ptr_cas->ptr_forward_plan = local_forward_plan;
      local_inverse_plan = 
	dist_fft_create_plan(DIST_FFT_1D_TYPE,
			     (dist_fft_dimension)(ptr_cas->nx),
			     DIST_FFT_INVERSE,
			     inverse_flags, MPI_COMM_WORLD);
      ptr_cas->ptr_inverse_plan = local_inverse_plan;
    } else if ((ptr_cas->nz) == 1) {
        if ((ptr_cas->nx) != ((ptr_cas->ny))) {
	fprintf(stderr,"dist_fft() must have nx=ny in 2D\n");
	exit(1);
      } else {
	local_forward_plan =  
	  dist_fft_create_plan(DIST_FFT_2D_TYPE,
			       (dist_fft_dimension)(ptr_cas->nx),
			       DIST_FFT_FORWARD,
			       forward_flags, MPI_COMM_WORLD);
        ptr_cas->ptr_forward_plan = local_forward_plan;
	local_inverse_plan = 
	  dist_fft_create_plan(DIST_FFT_2D_TYPE,
			       (dist_fft_dimension)(ptr_cas->nx),
			       DIST_FFT_INVERSE,
			       inverse_flags, MPI_COMM_WORLD);
	ptr_cas->ptr_inverse_plan = local_inverse_plan;
      }
    } else {
      if (((ptr_cas->nx) != (ptr_cas->ny)) ||
	  ((ptr_cas->nx) != ((ptr_cas->nz)))) {
	fprintf(stderr,"dist_fft() must have nx=ny=nz in 3D\n");
	exit(1);
      } else {
	local_forward_plan = 
	  dist_fft_create_plan(DIST_FFT_3D_TYPE,
			       (dist_fft_dimension)ptr_cas->nx,
			       DIST_FFT_FORWARD,forward_flags,
			       MPI_COMM_WORLD);
	ptr_cas->ptr_forward_plan = local_forward_plan;
	local_inverse_plan = 
	  dist_fft_create_plan(DIST_FFT_3D_TYPE,
			       (dist_fft_dimension)ptr_cas->nx,
			       DIST_FFT_INVERSE,
			       inverse_flags,MPI_COMM_WORLD);
	ptr_cas->ptr_inverse_plan = local_inverse_plan;
      }
    }
    MPI_Barrier(MPI_COMM_WORLD);
    
#else /* for FFTW */
    /* First create local copy to determine the best plan */
    local_copy.npix = ptr_cas->npix;
    DM_ARRAY_COMPLEX_STRUCT_INIT((&local_copy), local_copy.npix,p);
    dm_array_copy_complex(&local_copy,ptr_cas);

    /* This is the case of using the FFTW routines.
       Note that the default is DM_ARRAY_FFT_PATIENT so we only
       need to test for MEASURE and ESTIMATE
    */
    printf("starting FFTW\n");
    if ((fft_options & DM_ARRAY_FFT_ESTIMATE) ==
	DM_ARRAY_FFT_ESTIMATE) {
      fftw_flags = FFTW_ESTIMATE;
    } else if ((fft_options & DM_ARRAY_FFT_MEASURE) ==
	       DM_ARRAY_FFT_MEASURE) {
      fftw_flags = FFTW_MEASURE;
    } else {
      fftw_flags = FFTW_PATIENT;
    }

    /* Check the dimensionality of the input array and determine
     * if we need double or single precision. Then create the plans.
     */
    if (((ptr_cas->ny) == 1) && ((ptr_cas->nz) == 1)) {
#if defined(DM_ARRAY_DOUBLE)
      local_forward_plan =
	fftw_plan_dft_1d((int)(ptr_cas->nx),
			 (fftw_complex *)local_copy.complex_array,
			 (fftw_complex *)local_copy.complex_array,
			 FFTW_FORWARD, fftw_flags);
      ptr_cas->ptr_forward_plan = local_forward_plan;
      local_inverse_plan =
	fftw_plan_dft_1d((int)(ptr_cas->nx),
			 (fftw_complex *)local_copy.complex_array,
			 (fftw_complex *)local_copy.complex_array,
			 FFTW_BACKWARD, fftw_flags);
      ptr_cas->ptr_inverse_plan = local_inverse_plan;
#else
      local_forward_plan =
	fftwf_plan_dft_1d((int)(ptr_cas->nx),
			  (fftwf_complex *)local_copy.complex_array,
			  (fftwf_complex *)local_copy.complex_array,
			  FFTW_FORWARD, fftw_flags);
      ptr_cas->ptr_forward_plan = local_forward_plan;
      local_inverse_plan =
	fftwf_plan_dft_1d((int)(ptr_cas->nx),
			  (fftwf_complex *)local_copy.complex_array,
			  (fftwf_complex *)local_copy.complex_array,
			  FFTW_BACKWARD, fftw_flags);
      ptr_cas->ptr_inverse_plan = local_inverse_plan;
#endif
    } else if ((ptr_cas->nz) == 1) {
#if defined(DM_ARRAY_DOUBLE)
      local_forward_plan =
	fftw_plan_dft_2d((int)(ptr_cas->nx), (int)(ptr_cas->ny),
			 (fftw_complex *)local_copy.complex_array,
			 (fftw_complex *)local_copy.complex_array,
			 FFTW_FORWARD, fftw_flags);
      ptr_cas->ptr_forward_plan = local_forward_plan;
      local_inverse_plan =
	fftw_plan_dft_2d((int)(ptr_cas->nx), (int)(ptr_cas->ny),
			 (fftw_complex *)local_copy.complex_array,
			 (fftw_complex *)local_copy.complex_array,
			 FFTW_BACKWARD, fftw_flags);
      ptr_cas->ptr_inverse_plan = local_inverse_plan;
#else
      local_forward_plan =
	fftwf_plan_dft_2d((int)(ptr_cas->nx), (int)(ptr_cas->ny),
			  (fftwf_complex *)(local_copy.complex_array),
			  (fftwf_complex *)(local_copy.complex_array),
			  FFTW_FORWARD, fftw_flags);
      ptr_cas->ptr_forward_plan = local_forward_plan;
      local_inverse_plan =
	fftwf_plan_dft_2d((int)(ptr_cas->nx), (int)(ptr_cas->ny),
			  (fftwf_complex *)(local_copy.complex_array),
			  (fftwf_complex *)(local_copy.complex_array),
			  FFTW_BACKWARD, fftw_flags);
      ptr_cas->ptr_inverse_plan = local_inverse_plan;
#endif
    } else {
#if defined(DM_ARRAY_DOUBLE)
      local_forward_plan =
	fftw_plan_dft_3d((int)(ptr_cas->nx), (int)(ptr_cas->ny),
			 (int)(ptr_cas->nz),
			 (fftw_complex *)local_copy.complex_array,
			 (fftw_complex *)local_copy.complex_array,
			 FFTW_FORWARD, fftw_flags);
      ptr_cas->ptr_forward_plan = local_forward_plan;
      local_inverse_plan =
	fftw_plan_dft_3d((int)(ptr_cas->nx), (int)(ptr_cas->ny),
			 (int)(ptr_cas->nz),
			 (fftw_complex *)local_copy.complex_array,
			 (fftw_complex *)local_copy.complex_array,
			 FFTW_BACKWARD, fftw_flags);
      ptr_cas->ptr_inverse_plan = local_inverse_plan;
#else
      local_forward_plan =
	fftwf_plan_dft_3d((int)(ptr_cas->nx), (int)(ptr_cas->ny),
			  (int)(ptr_cas->nz),
			  (fftwf_complex *)local_copy.complex_array,
			  (fftwf_complex *)local_copy.complex_array,
			  FFTW_FORWARD, fftw_flags);
      ptr_cas->ptr_forward_plan = local_forward_plan;
      local_inverse_plan =
	fftwf_plan_dft_3d((int)(ptr_cas->nx), (int)(ptr_cas->ny),
			  (int)(ptr_cas->nz),
			  (fftwf_complex *)local_copy.complex_array,
			  (fftwf_complex *)local_copy.complex_array,
			  FFTW_BACKWARD, fftw_flags);
      ptr_cas->ptr_inverse_plan = local_inverse_plan;
#endif
    }
    DM_ARRAY_COMPLEX_FREE(local_copy.complex_array);
#endif /* End of dist_fft/FFTW creating plan*/
   
    if ((local_forward_plan == NULL) ||
	(local_inverse_plan == NULL)) {
      fprintf(stderr, "Error creating FFT plan\n");
      exit(1);
    }
  } /* End of create plan */

  /* This is the actual FFT section */
  if (((fft_options & DM_ARRAY_FORWARD_FFT) ==
       DM_ARRAY_FORWARD_FFT) || 
      ((fft_options & DM_ARRAY_INVERSE_FFT) ==
       DM_ARRAY_INVERSE_FFT)) {
#if (defined(__APPLE__) && defined(DIST_FFT))
    /* We want to execute an FFT */

    if ((fft_options & DM_ARRAY_FORWARD_FFT) ==
	DM_ARRAY_FORWARD_FFT) {
        dist_fft_local_dimensions(ptr_cas->ptr_forward_plan,
                                  &forward_dimension,
                                  &forward_start,&forward_storage_size);
        /* use DIST_FFT_MALLOC_WORKSPACE instead of
         * DM_ARRAY_COMPLEX_MALLOC
         */
        DIST_FFT_MALLOC_WORKSPACE(workspace,forward_storage_size);
        
        if (workspace == NULL) {
            printf("Unable to allocate memory. \n");
            exit(1);
        }
        dist_fft_warmup_workspace(ptr_cas->ptr_forward_plan,workspace);

        /* If the arrays aren't already distributed then we need to 
         * tell each process what its array is
         */
        //MPI_Barrier(MPI_COMM_WORLD);
        
        dist_fft_execute(ptr_cas->ptr_forward_plan,ptr_cas->complex_array,
                         workspace);
    }
    
    if ((fft_options & DM_ARRAY_INVERSE_FFT) ==
	DM_ARRAY_INVERSE_FFT) {
        //printf("Doing inverse transform from process %d,\n",my_rank);
        dist_fft_local_dimensions(ptr_cas->ptr_inverse_plan,
                                  &inverse_dimension,
                                  &inverse_start,&inverse_storage_size);

        /* use DIST_FFT_MALLOC_WORKSPACE instead of
         * DM_ARRAY_COMPLEX_MALLOC
         */
        DIST_FFT_MALLOC_WORKSPACE(workspace,inverse_storage_size);
      if (workspace == NULL) {
          printf("Unable to allocate memory. \n");
          exit(1);
      }
      dist_fft_warmup_workspace(ptr_cas->ptr_inverse_plan,workspace);
      
      //MPI_Barrier(MPI_COMM_WORLD);

      
      dist_fft_execute(ptr_cas->ptr_inverse_plan,ptr_cas->complex_array,
		       workspace);
    }
    //MPI_Barrier(MPI_COMM_WORLD);
    
    /* renormalization */
    for(ic=0; ic<local_size; ic++){
      c_re(ptr_cas->complex_array,ic) *= norm_factor;
      c_im(ptr_cas->complex_array,ic) *= norm_factor;
    }
    /* Need to use DIST_FFT version since we used it to allocate
     * the workspace.
     */
    DIST_FFT_FREE_WORKSPACE(workspace);
    
    
#else /* We use fftw */

    if ((fft_options & DM_ARRAY_FORWARD_FFT) ==
	DM_ARRAY_FORWARD_FFT) {
#if defined(DM_ARRAY_DOUBLE)
      fftw_execute_dft(ptr_cas->ptr_forward_plan,
		       ptr_cas->complex_array,
		       ptr_cas->complex_array);
#else
      fftwf_execute_dft(ptr_cas->ptr_forward_plan,
			ptr_cas->complex_array,
			ptr_cas->complex_array);
#endif

      /* after forward transform shift so that 0 frequency is at 
       * center of array.
       * 
      if (shift) {
	data_shift.npix = ptr_cas->npix;
	DM_ARRAY_COMPLEX_STRUCT_INIT((&data_shift),data_shift.npix,p);
	
	for(iz=0; iz<ptr_cas->nz; iz++){
	  jz = iz + ptr_cas->nz/2;
	  if( jz > (ptr_cas->nz-1)){
	    jz = jz - ptr_cas->nz;
	  }
	  for(iy=0; iy<ptr_cas->ny; iy++){
	    jy = iy + ptr_cas->ny/2;
	    if( jy > (ptr_cas->ny-1)){
	      jy = jy - ptr_cas->ny;
	    }
	    for(ix=0; ix<ptr_cas->nx; ix++){
	      jx = ix + ptr_cas->nx/2;
	      if( jx > (ptr_cas->nx-1)){
		jx = jx - ptr_cas->nx;
	      }
	      index =  
		ix + iy * ptr_cas->nx + iz * ptr_cas->nx * ptr_cas->ny;
	      index_shift =  
		jx + jy * ptr_cas->nx + jz * ptr_cas->nx * ptr_cas->ny;
	      c_re(data_shift.complex_array,index_shift) = 
		c_re(ptr_cas->complex_array,index);
	      c_im(data_shift.complex_array,index_shift) = 
		c_im(ptr_cas->complex_array,index);
	    }
	  }
	}
	dm_array_copy_complex(ptr_cas,&data_shift);
	DM_ARRAY_COMPLEX_FREE(data_shift.complex_array);
	}*/
    }
    
    
    if ((fft_options & DM_ARRAY_INVERSE_FFT) ==
	DM_ARRAY_INVERSE_FFT) {

      /* before inverse transform shift so that 0 frequency is at 
       * 0 index position.
       *
      data_shift.npix = ptr_cas->npix;
      DM_ARRAY_COMPLEX_STRUCT_INIT((&data_shift),data_shift.npix,p);
      
      for(iz=0; iz<ptr_cas->nz; iz++){
	jz = iz - ptr_cas->nz/2;
	if( jz < 0){
	  jz = jz + ptr_cas->nz;
	}
	for(iy=0; iy<ptr_cas->ny; iy++){
	  jy = iy - ptr_cas->ny/2;
	  if( jy < 0){
	    jy = jy + ptr_cas->ny;
	  }
	  for(ix=0; ix<ptr_cas->nx; ix++){
	    jx = ix - ptr_cas->nx/2;
	    if( jx < 0){
	      jx = jx + ptr_cas->nx;
	    }
	    index =  
	      ix + iy * ptr_cas->nx + iz * ptr_cas->nx * ptr_cas->ny;
	    index_shift =  
	      jx + jy * ptr_cas->nx + jz * ptr_cas->nx * ptr_cas->ny;
	    c_re(data_shift.complex_array,index_shift) = 
	      c_re(ptr_cas->complex_array,index);
	    c_im(data_shift.complex_array,index_shift) = 
	      c_im(ptr_cas->complex_array,index);
	  }
	}
      }
      dm_array_copy_complex(ptr_cas,&data_shift);
      DM_ARRAY_COMPLEX_FREE(data_shift.complex_array);*/

#if defined(DM_ARRAY_DOUBLE)
      fftw_execute_dft(ptr_cas->ptr_inverse_plan,
		       ptr_cas->complex_array,
		       ptr_cas->complex_array);
#else
      fftwf_execute_dft(ptr_cas->ptr_inverse_plan,
			ptr_cas->complex_array,
			ptr_cas->complex_array);
#endif
    }
    /* renormalization */
    dm_array_multiply_real_scalar(ptr_cas,norm_factor);
#endif /* End of dist_fft/FFTW ifdef */
  } /* FFT section */
}


