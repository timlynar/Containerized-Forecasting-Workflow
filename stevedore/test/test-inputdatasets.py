import unittest
from stevedore import *
from datetime import datetime
import logging
import os

"""
Basic unit testing for all input data.
TODO: make all dates relative to today.
NOTE: This is time consuming to execute by virtue of downloading one file of each type.

Tests:
 - all functions related to acquiring data;
 - the generation of filenames for each data sets;
 - the download of a file fore each data set.
"""

#Basic unit testing for all input data.
#This test may take some time.
#You will need to have your environment variables set to test RDA based datasets

class TestUM(unittest.TestCase):

    #setup some console logging so I can debug.
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s: %(message)s', datefmt='%d/%m/%Y %I:%M:%S %p')
    console = logging.StreamHandler()
    console.setLevel(logging.DEBUG)


    #setup
    def setUp(self):
        pass

    def test_download(self):
        """Test download from a single url (from FTP)"""

        # if exists /tmp/robots.txt delete it.
        ret = False
        if os.path.isfile('/tmp/robots.txt'):
            os.remove('/tmp/robots.txt')


        date_test = datetime.strptime('2017-01-01:12', '%Y-%m-%d:%H')

        #date, hour, path
        testds = InputDataSet(date_test, 0, '/tmp')
        testds.name ='robots.txt'
        testds.path = '/tmp'
        testds.name_prepared = 'NO-NAME'
        testds.alt_server_url = None
        testds.is_rda = [False]
        testds.server_path = ['']
        testds.server_url = ['ftp://nomads.ncdc.noaa.gov']
        testds.download()

        #if exists /tmp/robots.txt then pass.
        if os.path.isfile('/tmp/robots.txt') and os.path.getsize('/tmp/robots.txt') > 0:
            #os.remove('/tmp/robots.txt')
            ret = True

        self.assertEqual( ret, True)


    def test_download_grib(self):
        """Test download from a single url (from FTP)
        This file is a grib file.
        using: ftp://nomads.ncdc.noaa.gov/GFS/Grid3/201706/20170627/gfs_3_20170627_0000_000.grb2

        """
        # if the file alread exists delete it.
        ret = False
        if os.path.isfile('/tmp/gfs_3_20170627_0000_000.grb2'):
            os.remove('/tmp/gfs_3_20170627_0000_000.grb2')

        date_test = datetime.strptime('2017-01-01:12', '%Y-%m-%d:%H')
        #date, hour, path
        testds = InputDataSet(date_test, 0, '/tmp')
        testds.name ='gfs_3_20170627_0000_000.grb2'
        testds.path = '/tmp'
        testds.name_prepared = 'NO-NAME'
        testds.alt_server_url = None
        testds.is_rda = [False]
        testds.server_path = ['GFS/Grid3/201706/20170627']
        testds.server_url = ['ftp://nomads.ncdc.noaa.gov']
        testds.download()

        #if exists /tmp/robots.txt then pass.
        if os.path.isfile('/tmp/gfs_3_20170627_0000_000.grb2') and os.path.getsize('/tmp/gfs_3_20170627_0000_000.grb2') > 0:
            os.remove('/tmp/gfs_3_20170627_0000_000.grb2')
            ret = True
        self.assertEqual( ret, True)


    def test_download_from_second_server(self):
        """Test download from the second url
        The first url will go to nowhere
        This file is a grib file.
        using: ftp://nomads.ncdc.noaa.gov/GFS/Grid3/201706/20170627/gfs_3_20170627_0000_000.grb2
        """
        # if the file alread exists delete it.
        ret = False
        if os.path.isfile('/tmp/gfs_3_20170627_0000_000.grb2'):
            os.remove('/tmp/gfs_3_20170627_0000_000.grb2')

        date_test = datetime.strptime('2017-01-01:12', '%Y-%m-%d:%H')
        #date, hour, path
        testds = InputDataSet(date_test, 0, '/tmp')
        testds.name ='gfs_3_20170627_0000_000.grb2'
        testds.path = '/tmp'
        testds.name_prepared = 'NO-NAME'
        testds.alt_server_url = None
        testds.is_rda = [False, False]
        testds.server_path = ['','GFS/Grid3/201706/20170627']
        testds.server_url = ['ftp://www.fakeaddress812fzp9.com/','ftp://nomads.ncdc.noaa.gov']
        testds.download()

        #if exists /tmp/robots.txt then pass.
        if os.path.isfile('/tmp/gfs_3_20170627_0000_000.grb2') and os.path.getsize('/tmp/gfs_3_20170627_0000_000.grb2') > 0:
            os.remove('/tmp/gfs_3_20170627_0000_000.grb2')
            ret = True
        self.assertEqual( ret, True)


    def test_download_from_alt_server(self):
        """ Test the setting of an alt server.
        Remember the server path is set to 'pub/'+self.type
        the server_url = self.alt_server_url
        using http://ftp.geogratis.gc.ca/pub/nrcan_rncan/vector/canvec/doc/Read_me.txt
        """

        # if exists /tmp/robots.txt delete it.
        ret = False
        if os.path.isfile('/tmp/Read_me.txt'):
            os.remove('/tmp/Read_me.txt')


        date_test = datetime.strptime('2017-01-01:12', '%Y-%m-%d:%H')

        #date, hour, path
        testds = InputDataSet(date_test, 0, '/tmp')
        testds.type ='nrcan_rncan/vector/canvec/doc'
        testds.name = 'Read_me.txt'
        testds.path = '/tmp'
        testds.name_prepared = 'NO-NAME'
        testds.alt_server_url = 'http://ftp.geogratis.gc.ca'
        testds.is_rda = []
        testds.server_path = []
        testds.server_url = []
        testds.download()

        #if exists /tmp/robots.txt then pass.
        if os.path.isfile('/tmp/Read_me.txt') and os.path.getsize('/tmp/Read_me.txt') > 0:
            #os.remove('/tmp/robots.txt')
            ret = True

        self.assertEqual( ret, True)



    def test_exists_zero_size(self):
         """Test if a file exists
            if either
            self.path+'/'+self.name or
            self.path+'/'+self.name_prepared
            exists and is non-zero in size then this should return true.
            This should test the case where the file exists but is of zero size.
            Expect: False
         """

         date_test = datetime.strptime('2017-01-01:12', '%Y-%m-%d:%H')

         #delete the file if it is already here
         if os.path.isfile('/tmp/testfile'):
             os.remove('/tmp/testfile')

         #create the file
         try:
             open('/tmp/testfile', 'a').close()
         except:
             print "Could not open file in append mode ERROR."

         #date, hour, path
         testds = InputDataSet(date_test, 0, '/tmp')
         testds.name = "testfile"
         testds.path = "/tmp"
         testds.name_prepared = ""

         ret = testds.exists()

         print('File size is '+ str(os.path.getsize('/tmp/testfile')))
         print('Value of return is '+ str(ret))

         self.assertEqual(ret, False)


    def test_exists_nonzero_size(self):
         """Test if a file exists
            if either
            self.path+'/'+self.name or
            self.path+'/'+self.name_prepared
            exists and is non-zero in size then this should return true.
            This should test the file existing and being non-zero in size.
            Expect: True
         """

         date_test = datetime.strptime('2017-01-01:12', '%Y-%m-%d:%H')

         #delete the file if it is already here
         output_file = '/tmp/testfile'
         if os.path.isfile(output_file):
             os.remove(output_file)

         #create the file
         try:
              f = open(output_file, 'w')
              f.write('random text')
              f.close()
         except:
             print "Could not open file in append mode ERROR."

         #date, hour, path
         testds = InputDataSet(date_test, 0, '/tmp')
         testds.name = "testfile"
         testds.path = "/tmp"
         testds.name_prepared = ""

         ret = testds.exists()

         self.assertEqual( ret, True)


    def test_rda_login(self):
        """Test that we can login to RDA
        Note that this will only work if RDA_EMAIL and RDA_PASS are set
        """
        ret = False

        date_test = datetime.strptime('2017-01-01:12', '%Y-%m-%d:%H')
        #date, hour, path
        testds = InputDataSet(date_test, 0, '/tmp')

        if os.path.isfile(testds.RDA_LOGIN_PATH+'/login'):
            os.remove(testds.RDA_LOGIN_PATH+'/login')

        testds.rda_login()

        if os.path.isfile(testds.RDA_LOGIN_PATH+'/login'):
            #Get the first line of the file.
            #That line should not contain "Error: bad action"
            #The line should contain: "Authentication successful."
            #For some reason it can take a few minutes to run this login command.
            cfile = open(testds.RDA_LOGIN_PATH+'/login', 'r')
            content = cfile.read()
            if "successful" in content:
                ret = True

        #Assert that login was a success.
        self.assertEqual( ret, True)


    def test_InputDataSetGFSFCST_get_filename(self):
        date_test = datetime.strptime('2017-03-01:12', '%Y-%m-%d:%H')

        #date, hour, path
        testds = InputDataSetGFSFCST(date_test, 0, '/tmp')
        #looking for:  # 17030112.gfs.t12z.0p50.pgrb2f000
        expected_filename = "17030112.gfs.t12z.0p50.pgrb2f000"
        gen_filename = testds.get_filename()

        self.assertEqual( expected_filename, gen_filename)

    def test_InputDataSetGFS_get_filename(self):
        date_test = datetime.strptime('2017-03-01:12', '%Y-%m-%d:%H')
        #date, hour, path
        testds = InputDataSetGFS(date_test, 0, '/tmp')
        #looking for: ftp://nomads.ncdc.noaa.gov/GFS/Grid4/201703/20170301/gfs_4_20170301_1200_000.grb2
        #
        expected_filename = "gfs_4_20170301_1200_000.grb2"
        gen_filename = testds.get_filename()

        self.assertEqual( expected_filename, gen_filename)

    def test_InputDataSetGFSp25_get_filename(self):
        date_test = datetime.strptime('2017-03-01:12', '%Y-%m-%d:%H')
        #date, hour, path
        testds = InputDataSetGFSp25(date_test, 0, '/tmp')
        #looking for: https://rda.ucar.edu/data/ds084.1/2017/20170301/gfs.0p25.2017030112.f000.grib2
        expected_filename = "gfs.0p25.2017030112.f000.grib2"
        gen_filename = testds.get_filename()

        self.assertEqual( expected_filename, gen_filename)


    def test_InputDataSetFNL_get_filename(self):
        date_test = datetime.strptime('2017-03-01:12', '%Y-%m-%d:%H')
        #date, hour, path
        testds = InputDataSetFNL(date_test, 0, '/tmp')
        #looking for: https://rda.ucar.edu/data/ds083.2/grib2/2017/2017.03/fnl_20170301_12_00.grib2
        expected_filename = "fnl_20170301_12_00.grib2"
        gen_filename = testds.get_filename()

        self.assertEqual( expected_filename, gen_filename)

    def test_InputDataSetFNLp25_get_filename(self):
        date_test = datetime.strptime('2017-03-01:12', '%Y-%m-%d:%H')
        #date, hour, path
        testds = InputDataSetFNLp25(date_test, 0, '/tmp')
        #looking for: https://rda.ucar.edu/data/ds083.3/2017/201703/gdas1.fnl0p25.2017030112.f00.grib2
        expected_filename = "gdas1.fnl0p25.2017030112.f00.grib2"
        gen_filename = testds.get_filename()

        self.assertEqual( expected_filename, gen_filename)


    def test_InputDataSetCFSR_get_filename(self):
        #Note the last year of data for this dataset is 2016.
        date_test = datetime.strptime('2016-03-01:12', '%Y-%m-%d:%H')
        #date, hour, path
        testds = InputDataSetCFSR(date_test, 0, '/tmp')
        #looking for: http://soostrc.comet.ucar.edu/data/grib/cfsr/2016/03/16030112.cfsrr.t12z.pgrb2f00
        expected_filename = "16030112.cfsrr.t12z.pgrb2f00"
        gen_filename = testds.get_filename()

        self.assertEqual( expected_filename, gen_filename)

    def test_InputDataSetCFDDA_get_filename(self):
        #Note the last year of data for this dataset is 2005.
        #Using 2014-03-01 UTC 12 as the date.
        date_test = datetime.strptime('2004-03-01:12', '%Y-%m-%d:%H')
        #date, hour, path
        testds = InputDataSetCFDDA(date_test, 0, '/tmp')
        #looking for: https://rda.ucar.edu/data/ds604.0/2004/03/cfdda_2004030112.v2.nc
        expected_filename = "cfdda_2004030112.v2.nc"
        gen_filename = testds.get_filename()

        self.assertEqual( expected_filename, gen_filename)

    def test_InputDataSetRAP_get_filename(self):
        date_test = datetime.strptime('2017-03-01:12', '%Y-%m-%d:%H')
        #date, hour, path
        testds = InputDataSetRAP(date_test, 0, '/tmp')
        #looking for: http://soostrc.comet.ucar.edu/data/grib/rap/20170301/hybrid/17030112.rap.t12z.awp130bgrbf00.grib2
        expected_filename = "17030112.rap.t12z.awp130bgrbf00.grib2"
        gen_filename = testds.get_filename()

        self.assertEqual( expected_filename, gen_filename)

    def test_InputDataSetNAM_get_filename(self):
        date_test = datetime.strptime('2017-03-01:12', '%Y-%m-%d:%H')
        #date, hour, path
        testds = InputDataSetNAM(date_test, 0, '/tmp')
        #looking for: ftp://nomads.ncdc.noaa.gov/NAM/Grid218/201703/20170301/nam_218_20170301_1200_000.grb
        expected_filename = "nam_218_20170301_1200_000.grb"
        gen_filename = testds.get_filename()

        self.assertEqual( expected_filename, gen_filename)

    def test_InputDataSetSSTNCEP_get_filename(self):
        date_test = datetime.strptime('2017-03-01:12', '%Y-%m-%d:%H')
        #date, hour, path
        testds = InputDataSetSSTNCEP(date_test, 0, '/tmp')
        #looking for: ftp://polar.ncep.noaa.gov/pub/history/sst/ophi/rtg_sst_grb_hr_0.083.20170301
        expected_filename = "rtg_sst_grb_hr_0.083.20170301"
        gen_filename = testds.get_filename()

        self.assertEqual( expected_filename, gen_filename)

    def test_InputDataSetSSTOISST_get_filename(self):
        date_test = datetime.strptime('2017-03-01:12', '%Y-%m-%d:%H')
        #date, hour, path
        testds = InputDataSetSSTOISST(date_test, 0, '/tmp')
        #looking for: ftp://eclipse.ncdc.noaa.gov/pub/oisst/NetCDF/2017/AVHRR/avhrr-only-v2.20170301.nc.gz
        #Note if it is on ncei then we expect.
        #https://www.ncei.noaa.gov/data/sea-surface-temperature-optimum-interpolation/access/avhrr-only/201703/avhrr-only-v2.20170301.nc
        expected_filename = "avhrr-only-v2.20170301.nc"
        gen_filename = testds.get_filename()

        self.assertEqual( expected_filename, gen_filename)

    def test_InputDataSetSSTJPL_get_filename(self):
        date_test = datetime.strptime('2017-03-01:12', '%Y-%m-%d:%H')
        #date, hour, path
        testds = InputDataSetSSTJPL(date_test, 0, '/tmp')
        #Note: date_test.strftime("%j") = 060 for 2017-03-01:12
        #looking for: ftp://podaac-ftp.jpl.nasa.gov/allData/ghrsst/data/L4/GLOB/JPL_OUROCEAN/G1SST/2017/060/20170301-JPL_OUROCEAN-L4UHfnd-GLOB-v01-fv01_0-G1SST.nc.bz2
        expected_filename = "20170301-JPL_OUROCEAN-L4UHfnd-GLOB-v01-fv01_0-G1SST.nc.bz2"
        gen_filename = testds.get_filename()

        self.assertEqual( expected_filename, gen_filename)

    def test_InputDataSetSSTSPORT_get_filename(self):
        date_test = datetime.strptime('2017-03-01:12', '%Y-%m-%d:%H')
        #date, hour, path
        testds = InputDataSetSSTSPORT(date_test, 0, '/tmp')
        #looking for: http://soostrc.comet.ucar.edu/data/grib/sst/17030106.sportsst_nhemis.grb2 (only 06 or 18)
        expected_filename = "17030106.sportsst_nhemis.grb2.gz"
        gen_filename = testds.get_filename()

        self.assertEqual( expected_filename, gen_filename)

    def test_InputDataSetNASALISCONUS_get_filename(self):
        date_test = datetime.strptime('2017-03-01:12', '%Y-%m-%d:%H')
        #date, hour, path
        testds = InputDataSetNASALISCONUS(date_test, 0, '/tmp')
        #looking for:ftp://geo.msfc.nasa.gov/SPoRT/modeling/lis/conus3km/sportlis_conus3km_model_20170301_1200.grb2
        expected_filename = "sportlis_conus3km_model_20170301_1200.grb2"
        gen_filename = testds.get_filename()

        self.assertEqual( expected_filename, gen_filename)

    def test_InputDataSetNASAGF_get_filename(self):
        date_test = datetime.strptime('2017-03-01:12', '%Y-%m-%d:%H')
        #date, hour, path
        testds = InputDataSetNASAGF(date_test, 0, '/tmp')
        #looking for: ftp://geo.msfc.nasa.gov/SPoRT/modeling/viirsgvf/global/00001-10000.00001-05000.20170301.bz2
        expected_filename = "00001-10000.00001-05000.20170301.bz2"
        gen_filename = testds.get_filename()

        self.assertEqual( expected_filename, gen_filename)

    def test_InputDataSetMESONET_get_filename(self):
        date_test = datetime.strptime('2017-03-01:12', '%Y-%m-%d:%H')
        #date, hour, path
        testds = InputDataSetMESONET(date_test, 0, '/tmp')
        #looking for: ftp://madis-data.ncep.noaa.gov/archive/2017/03/01/LDAD/mesonet/netCDF/20170301_1200.gz
        expected_filename = "20170301_1200.gz"
        gen_filename = testds.get_filename()

        self.assertEqual( expected_filename, gen_filename)

    def test_InputDataSetMETAR_get_filename(self):
        date_test = datetime.strptime('2017-03-01:12', '%Y-%m-%d:%H')
        #date, hour, path
        testds = InputDataSetMETAR(date_test, 0, '/tmp')
        #looking for: ftp://madis-data.ncep.noaa.gov/archive/2017/03/01/point/metar/netcdf/20170301_1200.gz
        expected_filename = "20170301_1200.gz"
        gen_filename = testds.get_filename()

        self.assertEqual( expected_filename, gen_filename)

    def test_InputDataSetPREPBufr_get_filename(self):
        date_test = datetime.strptime('2017-03-01:12', '%Y-%m-%d:%H')
        #date, hour, path
        testds = InputDataSetPREPBufr(date_test, 0, '/tmp')
        #looking for: https://rda.ucar.edu/data/ds337.0/tarfiles/2017/prepbufr.20170301.nr.tar.gz
        expected_filename = "prepbufr.20170301.nr.tar.gz"
        gen_filename = testds.get_filename()

        self.assertEqual( expected_filename, gen_filename)

    def test_InputDataSetLittleRSurface_get_filename(self):
        date_test = datetime.strptime('2017-03-01:12', '%Y-%m-%d:%H')
        #date, hour, path
        testds = InputDataSetLittleRSurface(date_test, 0, '/tmp')
        #looking for: https://rda.ucar.edu/data/ds461.0/little_r/2017/SURFACE_OBS:2017030112
        expected_filename = "SURFACE_OBS:2017030112"
        gen_filename = testds.get_filename()

        self.assertEqual( expected_filename, gen_filename)

    def test_InputDataSetLittleRUpperAir_get_filename(self):
        date_test = datetime.strptime('2017-03-01:12', '%Y-%m-%d:%H')
        #date, hour, path
        testds = InputDataSetLittleRUpperAir(date_test, 0, '/tmp')
        #looking for: https://rda.ucar.edu/data/ds351.0/little_r/2017/OBS:2017030112
        expected_filename = "OBS:2017030112"
        gen_filename = testds.get_filename()

        self.assertEqual( expected_filename, gen_filename)

if __name__ == '__main__':
    unittest.main()
