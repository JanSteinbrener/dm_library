/* This is the file dm_fileio.h */

#ifndef DM_FILEIO_H
#define DM_FILEIO_H

#include <hdf5.h>

#ifdef __cplusplus
extern "C"
{
#endif /* __cplusplus */

#include "dm.h"
  
#define DM_FILEIO_SUCCESS 0
#define DM_FILEIO_FAILURE (-1)
#define PI 3.14159256

  /* Creating a new HDF 5 file for writing */
  int dm_h5_create(char *filename, hid_t *ptr_h5_file_id,
                   char *error_string, int my_rank);

  /* Open (and overwrite) an HDF 5 file for writing */
  int dm_h5_openwrite(char *filename, hid_t *ptr_h5_file_id,
		      char *error_string, int my_rank);

  /* Open readonly an HDF 5 file */
  int dm_h5_openread(char *filename, hid_t *ptr_h5_file_id,
		     char *error_string, int my_rank);
  
  /* Close an HDF 5 file */
    void dm_h5_close(hid_t h5_file_id, int my_rank);
  
  /* Create comments for an already-opened HDF 5 file.
   */
  int dm_h5_create_comments(hid_t h5_file_id,
			    dm_comment_struct *ptr_comment_struct,
			    char *error_string, int my_rank);

  /* Add comments for an already-opened HDF 5 file with existing comments
   * group.
   */
  int dm_h5_add_comments(hid_t h5_file_id,
			 dm_comment_struct *ptr_comment_struct,
			 char *error_string, int my_rank);
  
  /* Add the assembly info structure (ainfo).
   */
  int dm_h5_write_ainfo(hid_t h5_file_id,
			dm_ainfo_struct *ptr_ainfo_struct,
			char *error_string, int my_rank);
  
  /* Add adi (assembled diffraction intensities) to an already-
   * opened HDF 5 file.  If adi_error_array.npix=0, then no
   * adi_error_array will be added to the file.
   */
  int dm_h5_write_adi(hid_t h5_file_id,
		      dm_adi_struct *ptr_adi_struct,
		      dm_array_real_struct *ptr_adi_array_struct,
		      dm_array_real_struct *ptr_adi_error_array_struct,
		      char *error_string, int my_rank, int p);
  

  /* Add spt (support mask) to an already-opened HDF 5 file.  
   */
  int dm_h5_write_spt(hid_t h5_file_id,
		      dm_spt_struct *ptr_spt_struct,
		      dm_array_byte_struct *ptr_spt_array_struct,
		      char *error_string, int my_rank, int p);
  
  /* Add itn (complex iterate) to an already-opened HDF 5 file.  
   */
  int dm_h5_write_itn(hid_t h5_file_id,
		      dm_itn_struct *ptr_itn_struct,
		      dm_array_complex_struct *ptr_itn_array_struct,
		      dm_array_real_struct *ptr_recon_errors,
		      char *error_string,
                      int my_rank,
                      int p);
  
  /* This routine reads in the size of the comment string array */
  int dm_h5_read_comments_info(hid_t h5_file_id,
			       int *ptr_n_strings, int *ptr_string_length,
			       char *error_string,
                               int my_rank);
  
  /* This routine reads in the comments from an already-opened HDF 5 file */
  int dm_h5_read_comments(hid_t h5_file_id,
			  dm_comment_struct *ptr_comment_struct,
			  char *error_string,
                          int my_rank);

  /* This routine determines the size of the filename_array */
  int dm_h5_read_ainfo_info(hid_t h5_file_id,
			    int *ptr_n_names, int *ptr_string_length,
			    char *error_string,
                            int my_rank);

  /* This routine reads in the filename and file directory 
   * from an already-opened HDF 5 file 
   */
  int dm_h5_read_ainfo(hid_t h5_file_id,
		       dm_ainfo_struct *ptr_ainfo_struct,
		       char *error_string,
                       int my_rank);
 
  /* This routine determines if an ADI group exists */
    int dm_h5_adi_group_exists(hid_t h5_file_id,int my_rank);
  /* This routine determines if an ADS group exists */
    int dm_h5_ads_group_exists(hid_t h5_file_id,int my_rank);
  /* This routine determines if an SPT group exists */
    int dm_h5_spt_group_exists(hid_t h5_file_id,int my_rank);
  /* This routine determines if an ITN group exists */
    int dm_h5_itn_group_exists(hid_t h5_file_id,int my_rank);
  /* This routine determines if a COMMENTS group exists */
    int dm_h5_comments_group_exists(hid_t h5_file_id,int my_rank);
  
  /* This routine reads the ADI structure and the size of the ADI array
   * from an already-opened HDF 5 file.  It also determines
   * if there is an adi_error_array in the file or not.  If adi_error_array
   * is not present, you should not allocate memory for it and you
   * should set nx=ny=nz=ppix=0 in any adi_error_array_struct you
   * might create.
   */
  int dm_h5_read_adi_info(hid_t h5_file_id,
			  int *ptr_nx, int *ptr_ny, int *ptr_nz,
			  int *ptr_error_is_present,
			  dm_adi_struct *ptr_adi_struct,
			  char *error_string,
                          int my_rank);

  /* This routine reads in adi_array_struct, and adi_error_array_struct.
   * If there is no adi_error_array_struct data to be read in, then 
   * nx=ny=nz=npix=0 is set in the structure.  If adi_error_array_struct.npix
   * is set to zero before calling this routine, then adi_error_array
   * will not be read in even if it is present in the file.
   */
  int dm_h5_read_adi(hid_t h5_file_id,
		     dm_array_real_struct *ptr_adi_array_struct,
		     dm_array_real_struct *ptr_adi_error_array_struct,
		     char *error_string,
                     int my_rank,
                     int p);

  /* This routine reads the SPT structure and the size of the SPT array
   * from an already-opened HDF 5 file.
   */
  int dm_h5_read_spt_info(hid_t h5_file_id,
			  int *ptr_nx, int *ptr_ny, int *ptr_nz,
			  dm_spt_struct *ptr_spt_struct,
			  char *error_string,
                          int my_rank);

  /* This routine reads in spt_array_struct.
   */
  int dm_h5_read_spt(hid_t h5_file_id,
		     dm_array_byte_struct *ptr_spt_array_struct,
		     char *error_string,
                     int my_rank,
                     int p);

  /* This routine reads the ITN structure and the size of the ITN array
   * from an already-opened HDF 5 file.
   */
  int dm_h5_read_itn_info(hid_t h5_file_id,
			  int *ptr_nx, int *ptr_ny, int *ptr_nz,
			  int *ptr_recon_errors_npix,
			  dm_itn_struct *ptr_itn_struct,
			  char *error_string,
                          int my_rank);

    /* This routine reads in itn_array_struct.
     */
    int dm_h5_read_itn(hid_t h5_file_id,
		       dm_array_real_struct *ptr_recon_errors,
                       dm_array_complex_struct *ptr_itn_array_struct,
                       char *error_string,
                       int my_rank,
                       int p);
    
    /* This routine clears the contents of the comment string array. */
    void dm_clear_comments(dm_comment_struct *ptr_comment_struct);
  
  /* This routine adds a string to the comment string array.  If the
   * length of the new string is beyond what can be accomodated,
   * the string_to_add is truncated.
   */
  void dm_add_string_to_comments(char *string_to_add,
				 dm_comment_struct *ptr_comment_struct);
  
  /* This routine updates "specimen_name" within the comments.
   */
  void dm_add_specimen_name_to_comments(char *string_to_add,
					dm_comment_struct *ptr_comment_struct);
  
  /* This routine updates "collection_date" within the comments.
   */
  void dm_add_collection_date_to_comments(char *string_to_add,
					  dm_comment_struct *ptr_comment_struct);

  /* This routine prints out the string_array line-by-line,
   * with a possible preceding and trailing string on each line.
   */
  void dm_print_comments(FILE *fp_out, 
			 char *preceding_string,
			 dm_comment_struct *ptr_comment_struct,
			 char *trailing_string,
                         int my_rank);
  
  /* This routine clears the contents of the ainfo arrays and offset
   * variables. It should be called whenever a new ainfo_struct is 
   * initialized.
   */
    void dm_clear_ainfo(dm_ainfo_struct *ptr_ainfo_struct);

  /* This routine adds the file directory that contains all the 
   * 2 dimensional files merged together.
   */
  void dm_add_file_directory_to_ainfo(char *directory_to_add,
				      dm_ainfo_struct *ptr_ainfo_struct);

  /* This routine adds a filename to the array that contains all
   * the filenames of 2 dimensional datasets merged together
   */
  void dm_add_filename_to_ainfo(char *filename_to_add,
				dm_ainfo_struct *ptr_ainfo_struct);
  
  /* This routine reads-in the whole ainfo_struct from a 
   * file containing tags and csv lists of values.
   */
  int dm_read_ainfo_from_csv(char *csv_filename,
			     dm_ainfo_struct *ptr_ainfo_struct,
			     char *error_string);

  /* This routine adds a double value to one of the double arrays 
   * in ainfo_struct specified by 'tagname'.
   */
  int dm_add_double_to_ainfo(char *tagname,
			     double double_to_add,
			     dm_ainfo_struct *ptr_ainfo_struct,
			     char *error_string);

  /* This routine adds a systime to the array of systimes
   */
  void dm_add_systime_to_ainfo(char *systime_to_add,
			       dm_ainfo_struct *ptr_ainfo_struct);

  /* This routine prints out the filename_array line-by-line,
   * with a possible preceding and trailing string on each line.
   */
  void dm_print_ainfo(FILE *fp_out, 
		      char *preceding_string,
		      dm_ainfo_struct *ptr_ainfo_struct,
		      char *trailing_string,
                      int my_rank);
  
  /* This routine clears a local_string_array. It is handy if we
   * want to use this local_string_array several times within 
   * one function
   */
  void dm_clear_local_string_array(char *local_string_array,
				   int string_length,
				   int n_frames);

  /* This routine checks if the arrays in ainfo_struct have the same
   * length. If not (i.e. user did not define some tags), then they 
   * are made the same length by adding default values for missing 
   * entries. No array can be longer than n_frames. Note: this function
   * is called automatically when writing ainfo_struct to HDF5 file
   */
  int dm_check_ainfo(dm_ainfo_struct *ptr_ainfo_struct,
		     char *error_string,
                     int my_rank);

  /* This internal routine does the H5Tinsert() calls to build up
   * the adi_struct variables.
   */
  void dm_h5_insert_adi_struct_members(hid_t datatype);
  /* This internal routine does the H5Tinsert() calls to build up
   * the spt_struct variables.
   */
  void dm_h5_insert_spt_struct_members(hid_t datatype);
  /* This internal routine does the H5Tinsert() calls to build up
   * the itn_struct variables.
   */
  void dm_h5_insert_itn_struct_members(hid_t datatype);
  
#ifdef __cplusplus
}  /* extern "C" */
#endif /* __cplusplus */

#endif /* #ifndef DM_FILEIO_H */
