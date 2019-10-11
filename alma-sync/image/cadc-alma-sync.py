#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ***********************************************************************
# ******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
# *************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
#
#  (c) 2019.                            (c) 2019.
#  Government of Canada                 Gouvernement du Canada
#  National Research Council            Conseil national de recherches
#  Ottawa, Canada, K1A 0R6              Ottawa, Canada, K1A 0R6
#  All rights reserved                  Tous droits réservés
#
#  NRC disclaims any warranties,        Le CNRC dénie toute garantie
#  expressed, implied, or               énoncée, implicite ou légale,
#  statutory, of any kind with          de quelque nature que ce
#  respect to the software,             soit, concernant le logiciel,
#  including without limitation         y compris sans restriction
#  any warranty of merchantability      toute garantie de valeur
#  or fitness for a particular          marchande ou de pertinence
#  purpose. NRC shall not be            pour un usage particulier.
#  liable in any event for any          Le CNRC ne pourra en aucun cas
#  damages, whether direct or           être tenu responsable de tout
#  indirect, special or general,        dommage, direct ou indirect,
#  consequential or incidental,         particulier ou général,
#  arising from the use of the          accessoire ou fortuit, résultant
#  software.  Neither the name          de l'utilisation du logiciel. Ni
#  of the National Research             le nom du Conseil National de
#  Council of Canada nor the            Recherches du Canada ni les noms
#  names of its contributors may        de ses  participants ne peuvent
#  be used to endorse or promote        être utilisés pour approuver ou
#  products derived from this           promouvoir les produits dérivés
#  software without specific prior      de ce logiciel sans autorisation
#  written permission.                  préalable et particulière
#                                       par écrit.
#
#  This file is part of the             Ce fichier fait partie du projet
#  OpenCADC project.                    OpenCADC.
#
#  OpenCADC is free software:           OpenCADC est un logiciel libre ;
#  you can redistribute it and/or       vous pouvez le redistribuer ou le
#  modify it under the terms of         modifier suivant les termes de
#  the GNU Affero General Public        la “GNU Affero General Public
#  License as published by the          License” telle que publiée
#  Free Software Foundation,            par la Free Software Foundation
#  either version 3 of the              : soit la version 3 de cette
#  License, or (at your option)         licence, soit (à votre gré)
#  any later version.                   toute version ultérieure.
#
#  OpenCADC is distributed in the       OpenCADC est distribué
#  hope that it will be useful,         dans l’espoir qu’il vous
#  but WITHOUT ANY WARRANTY;            sera utile, mais SANS AUCUNE
#  without even the implied             GARANTIE : sans même la garantie
#  warranty of MERCHANTABILITY          implicite de COMMERCIALISABILITÉ
#  or FITNESS FOR A PARTICULAR          ni d’ADÉQUATION À UN OBJECTIF
#  PURPOSE.  See the GNU Affero         PARTICULIER. Consultez la Licence
#  General Public License for           Générale Publique GNU Affero
#  more details.                        pour plus de détails.
#
#  You should have received             Vous devriez avoir reçu une
#  a copy of the GNU Affero             copie de la Licence Générale
#  General Public License along         Publique GNU Affero avec
#  with OpenCADC.  If not, see          OpenCADC ; si ce n’est
#  <http://www.gnu.org/licenses/>.      pas le cas, consultez :
#                                       <http://www.gnu.org/licenses/>.
#
#  $Revision: 4 $
#
# ***********************************************************************
#

from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import logging
from cadcutils import util, net, exceptions
import datetime
import tempfile
from io import StringIO
import cadctap
from cadcdata import CadcDataClient
from astroquery.alma import Alma
from astropy.time import Time
import threading
from queue import Queue
import copy

FORMAT = '%(thread)d %(asctime)-15s: %(message)s'
logger = logging.getLogger(__name__)

ALMA_MIRRORS = ['https://almascience.nrao.edu',
              ##  'https://almascience.eso.org',
                'https://almascience.nao.ac.jp']

job_queue = Queue()  # queue of incomplete observations to be downloaded


def str2date(s):
    """
    Takes a date formatted string and returns a datetime.

    """
    date_format = '%Y-%m-%dT%H:%M:%S'
    if s is None:
        return None
    return datetime.datetime.strptime(s, date_format)


class DownloadWorker():

    def __init__(self, subject, url, retry):
        self._alma = copy.deepcopy(Alma)
        self._alma.dataarchive_url = url
        self._cadc_data = CadcDataClient(subject)
        self.num_obs = 0
        self.total_size = 0
        self.retry = retry

    def run(self):
        while True:
            obs = job_queue.get()
            if obs is None:
                break
            logger.debug(
                'Downoading obs {} from {}'.format(obs[0],
                                                   self._alma.dataarchive_url))

            try:
                self.total_size += \
                    download(self._alma, self._cadc_data, obs[0], obs[1])
                self.num_obs += 1
            except Exception as e:
                logger.error('Failed to transfer obs {} - {}'.format(obs[0],
                                                                     str(e)))
                if self.retry:
                    logger.error('Throw {} back in the QUEUE'.format(obs[0]))
                    job_queue.put(obs)
                else:
                    logger.error('Failed to completly transfer obs {}'.
                                 format(obs))
            job_queue.task_done()


def get_artifacs_to_sync(subject, start=None, end=None, all=False,
                         incomplete=False):
    ad_client = cadctap.CadcTapClient(subject,
                                      resource_id='ivo://cadc.nrc.ca/ad')
    logging.debug('Query AD')
    tf = tempfile.NamedTemporaryFile(suffix='.xml')
    ad_client.query(
        ("select 'alma:ALMA/'+fileName as uri, fileSize, contentType from "
         "archive_files where archiveName='ALMA' and fileName like '%.tar'"),
        response_format='VOTable', output_file=tf.name)

    logging.debug('Query argus now')

    ams_client = cadctap.CadcTapClient(subject,
                                      resource_id='ivo://cadc.nrc.ca/ams/alma')
    result = StringIO()
    interval = ''
    if start:
        interval = ' and p.time_bounds_lower>{}'.format(Time(start).mjd)
    if end:
        interval += ' and p.time_bounds_lower<={}'.format(Time(end).mjd)

    if incomplete:
        incomplete_query = (
            'select distinct observationID from caom2.Observation o '
            'join caom2.Plane p on o.obsID=p.obsID join caom2.Artifact a on '
            'a.planeID=p.planeID left join tap_upload.ad ad on a.uri=ad.uri '
            'and a.contentLength=ad.fileSize and a.contentType=ad.contentType '
            'where ad.uri is Null')
        if not all:
            incomplete_query = (
                    "select observationID from caom2.Observation o join "
                    "caom2.Plane p on o.obsID=p.obsID join caom2.Artifact a "
                    "on p.planeID=a.planeID where observationID in ({}) and "
                    "uri like '%auxiliary.tar'".format(incomplete_query))
            
        sql = (
            'select observationID, a.uri, contentLength, a.contentType '
            'from caom2.Observation o '
            'join caom2.Plane p on o.obsID=p.obsID join caom2.Artifact a on '
            'a.planeID=p.planeID left join tap_upload.ad ad on a.uri=ad.uri '
            'and a.contentLength=ad.fileSize and a.contentType=ad.contentType '
            'where ad.uri is Null and observationID in ({})'.format(
                incomplete_query))
    else:
        sql = (
            'select observationID, a.uri, contentLength, a.contentType '
            'from caom2.Observation o '
            'join caom2.Plane p on o.obsID=p.obsID join caom2.Artifact a on '
            'a.planeID=p.planeID where observationID not in ('
            'select observationID from caom2.Observation o '
            'join caom2.Plane p on o.obsID=p.obsID join caom2.Artifact a on '
            'a.planeID=p.planeID left join tap_upload.ad ad on a.uri=ad.uri '
            'and a.contentLength=ad.fileSize and a.contentType=ad.contentType '
            'where ad.uri is Null)'
        )
    logger.debug('Executing ' + sql)
    ams_client.query(sql, output_file=result, response_format='csv',
                     tmptable='ad:' + tf.name, data_only=True)

    obs = {}
    for row in result.getvalue().split('\n'):
        elems = row.split(',')
        if len(elems) == 4 and elems[1].startswith('alma:ALMA'):
            file_name = elems[1]
            # file name is in the path of the uri
            file_name = file_name[file_name.find('/')+1:]
            if elems[0] in obs:
                obs[elems[0]][file_name] = elems[2:]
            else:
                obs[elems[0]] = {file_name: elems[2:]}

    if not all and not incomplete:
        # Filter out observations with no auxiliary files
        for ob in list(obs.keys()):
            found = False
            for file in obs[ob]:
                if 'auxiliary' in file:
                    found = True
                    break
            if not found:
                del obs[ob]
    return obs


def list_obs(subject, start=None, end=None, all=False, incomplete=False):
    """
    List complete observations, i.e. observations that have their artifacts
    in the CADC storage
    :param subject: subject performing action
    :param all: If True list all incomplete observations, otherwise just
    observations containing auxiliary files
    :param all: list all observations if True, only those with auxiliary
    artifacts otherwise
    :param incomplete: If true, list incomplete observations instead
    (observations with missing artifacts)
    :return:
    """
    obs = get_artifacs_to_sync(subject, start, end, all, incomplete)
    num_artifacts = 0
    total_size = 0
    for key in obs:
        print('{}'.format(key))
        for file in obs[key].items():
            if incomplete:
                print('\t{} ({:.2f}GB)'.format(file[0],
                                         int(file[1][0])/1024/1024/1024))
            total_size += int(file[1][0])/1024/1024/1024
        num_artifacts += len(obs[key])
    msg = 'Incomplete' if incomplete else 'Complete'
    print('\n{} observations: {} Observations, {} Artifacts ({:.2f}GB)'.
          format(msg, len(obs), num_artifacts, total_size))


def get_missing_files_obs(subject, obsid):
    ams_client = cadctap.CadcTapClient(subject,
                                      resource_id='ivo://cadc.nrc.ca/ams/alma')
    logger.debug('Query AMS')
    uris = StringIO()
    query = ("select uri, contentLength, contentType from caom2.Observation "
             "o join caom2.Plane p on "
             "o.obsID=p.obsID join caom2.Artifact a on a.planeID=p.planeID "
             "where observationID='{}'".format(obsid))
    logger.debug(query)
    ams_client.query(query, response_format='csv', output_file=uris)
    files = {}
    for row in uris.getvalue().split('\n'):
        if row.startswith('alma:ALMA'):
            fields = row.strip().split(',')
            files[fields[0].split('/')[1]] = fields[1:]

    logger.debug('AD now')
    ad_client = cadctap.CadcTapClient(subject,
                                      resource_id='ivo://cadc.nrc.ca/ad')
    result = StringIO()
    query = ("select fileName, fileSize, contentType from archive_files where "
             "archiveName='ALMA' and fileName in ('{}')".
             format("', '".join(files.keys())))
    logger.debug(query)
    ad_client.query(query=query, output_file=result, response_format='csv',
                    data_only=True)
    for row in result.getvalue().split('\n'):
        elem = row.split(',')
        # if match in file name and size -> file is present
        if elem[0] in files and elem[1] == files[elem[0]][0] and \
           elem[2] == files[elem[0]][1]:
            del files[elem[0]]
    return files


def check_obs(subject, obsid):
    not_received = get_missing_files_obs(subject, obsid)
    if not_received:
        print('{}: INCOMPLETE. Missing files:'.format(obsid))
        for i in not_received.items():
            print('\t{} ({:.2f}GB)'.format(i[0], int(i[1][0])/1024/1024/1024))
    else:
        print('{}: COMPLETE'.format(obsid))


def _to_member_ouss_id(obs_id):
    return 'uid://{}'.format(obs_id.replace('_', '/'))


def download(alma_client, data_client, obsid, missing_files):
    logger.info('Download {} files for obs {}'.format(len(missing_files),
                                                      obsid))
    if not missing_files:
        logger.warning("No missing files for observation {}".format(obsid))
        return 0
    files = alma_client.stage_data(_to_member_ouss_id(obsid))
    total_size = 0
    errors = False
    for file in list(missing_files.keys()):
        for alma_file in files:
            if alma_file['URL'].endswith(file):
                expected_size = int(missing_files[file][0])
                alma_size = int(alma_file['size']*1000**3)
                if expected_size != alma_size:
                    raise AttributeError(
                        'File {}/{} - Caom2 size ({}) != Alma size ({})'.
                    format(obsid, file, expected_size, alma_size))
                logger.info('Downloading file {} in obs {} from {}...'.
                            format(file, obsid, alma_file['URL']))
                try:
                    data_client.put_file('ALMA', alma_file['URL'],
                                         input_name=file,
                                         mime_type=missing_files[file][1],
                                         mime_encoding=None,
                                         md5_check=False)
                except Exception as e:
                    errors = True
                    logger.warning('Errors transferring obs/file'.format(obsid,
                                                                         file))
                    logger.debug('Error details: ' + str(e))
                    break
                total_size += expected_size
                del missing_files[file]
                break
    if errors:
        raise RuntimeError(obsid + ' not downloaded completely.')
    return total_size


def download_artifacts(subject, obsid, threads, start=None, end=None,
                       retry=False, all=False):
    """
    Download missing artifacts
    :param subject: subject performing action
    :param obsid: specific observation ID to download missing artifacts for.
    Download all missing artifacts if this is None
    :param threads: Number of threads per site
    :param retry: Retry to download observation if error occurs
    :param all: If True download all incomplete observations, otherwise just
    observations containing auxiliary files
    :return: 
    """
    start_proc = datetime.datetime.now()
    site_amount = {}
    site_num_obs = {}
    if obsid or not threads:
        logger.debug('Download with single thread')
        data_client = CadcDataClient(subject)
        site_amount[Alma._get_dataarchive_url()] = 0
        site_num_obs[Alma.dataarchive_url] = 0
        while True:
            try:
                if obsid:
                    missing_files = get_missing_files_obs(subject, obsid)
                    site_amount[Alma._get_dataarchive_url()] = \
                        download(Alma, data_client, obsid, missing_files)
                    site_num_obs[Alma.dataarchive_url] = 1
                else:
                    observations = get_artifacs_to_sync(subject, start, end,
                                                        all, True)
                    Alma._dataarchive_url = 'https://almascience.eso.org'
                    for obs in observations.keys():
                        site_amount[Alma.dataarchive_url] += \
                            download(Alma, data_client, obs, observations[obs])
                        site_num_obs[Alma.dataarchive_url] += 1
            except exceptions.UnauthorizedException as e:
                raise e
            except exceptions.HttpException as e1:
                if retry:
                    logger.warning(
                        'Failed to download {}. Retrying'.format(obsid))
                    logger.debug(str(e1))
                    continue
                else:
                    raise e1
            break
    else:
        # create the pool of workers
        # populate the queue
        for obs in get_artifacs_to_sync(subject, start, end,
                                        all, True).items():
            job_queue.put(obs)

        tpool = []
        workers = []
        for i in range(threads):
            for mirror in ALMA_MIRRORS:
                worker = DownloadWorker(subject, mirror, retry)
                workers.append(worker)
                t = threading.Thread(target=worker.run)
                t.start()
                tpool.append(t)
                site_amount[worker._alma.dataarchive_url] = 0
                site_num_obs[worker._alma.dataarchive_url] = 0
        logger.debug('Download with {} threads'.format(len(tpool)))

        # wait for job_queue to finish
        job_queue.join()

        duration = datetime.datetime.now() - start_proc
        # stop all the threads
        for i in range(len(tpool)):
            job_queue.put(None)

        for t in tpool:
            t.join()

        for w in workers:
            site_amount[w._alma.dataarchive_url] += w.total_size
            site_num_obs[w._alma.dataarchive_url] += w.num_obs

    print('Download report')
    seconds = (datetime.datetime.now() - start_proc).total_seconds()
    print('Duration: {}'.format(datetime.timedelta(seconds=int(seconds))))
    total_obs = 0
    total_size = 0
    for site in site_amount:
        total_obs += site_num_obs[site]
        total_size += site_amount[site]
        print('Site: ' + site)
        print('\tObservations: {}'.format(site_num_obs[site]))
        print('\t  Downloaded: {:.2f}GB'.
              format(round(site_amount[site]/1024/1024/1024, 3)))
        print('\t  Avg. speed: {:.2f}MB/s'.format(
            round(site_amount[site]/1024/1024/seconds, 3)))
    if len(site_amount) > 1:
        print('------------------')
        print('Total')
        print('\tObservations: {}'.format(total_obs))
        print('\tDownloaded: {:.2f}GB'.
              format(round(total_size/1024/1024/1024, 3)))
        print('\tAvg. speed: {:.2f}MB/s'.format(
            round(total_size/1024/1024/seconds, 3)))


def test(subject):
    import requests
    data = CadcDataClient(subject)
    data.put_file('TEST', 'https://almascience.nrao.edu/dataPortal/requests/anonymous/1650346716859/ALMA/2016.1.00101.S_uid___A001_X87a_X135_auxiliary.tar/2016.1.00101.S_uid___A001_X87a_X135_auxiliary.tar', mime_type='application/html',
                  md5_check=False, input_name='almatest2.tar')


def main():
    parser = util.get_base_parser(
        version=0.01,
        default_resource_id='ivo://cadc.nrc.ca/cadc_alma_sync')

    subparsers = parser.add_subparsers(
        dest='cmd',
        help='supported commands. Use the -h|--help argument of a command '
             'for more details')
    list_parser = subparsers.add_parser(
        'list',
        description='List ALMA observations and artifacts in CADC storage.',
        help='List ALMA observations and artifacts in CADC storage.')
    list_parser.add_argument(
        '-i', '--incomplete', action='store_true',
        help='Reverse - list only the incomplete observations.'
    )

    download_parser = subparsers.add_parser(
        'download',
        description='Download missing files in storage',
        help='Download missing files from ALMA to CADC'
    )
    # add arguments that are common to both list and download parsers
    for pars in [list_parser, download_parser]:
        pars.add_argument('-o', '--obsid', help='Observation ID')
        pars.add_argument('--start', type=str2date,
                          help='Start from observation date '
                               '(UTC IVOA format: YYYY-mm-ddTH:M:S)')
        pars.add_argument('--end', type=str2date,
                          help='End with observation date '
                               '(UTC IVOA format: YYYY-mm-ddTH:M:S)')
        pars.add_argument(
            '-a', '--all', action='store_true',
            help='Applly to all observations (with or without auxiliary files)'
        )

    download_parser.add_argument(
        '-t', '--threads', type=int,
        help='Number of download threads for each ALMA mirror')
    download_parser.add_argument(
        '-r', '--retry', action='store_true',
        help='Retry to download observation if error occurs'
    )
    test_parser = subparsers.add_parser(
        'test',
        description='Test',
        help='Test')

    args = parser.parse_args()
    if args.verbose:
        logging.basicConfig(level=logging.INFO, stream=sys.stdout,
                            format=FORMAT)
    elif args.debug:
        logging.basicConfig(level=logging.DEBUG, stream=sys.stdout,
                            format=FORMAT)
    else:
        logging.basicConfig(level=logging.WARN, stream=sys.stdout,
                            format=FORMAT)

    subject = net.Subject.from_cmd_line_args(args)

    if args.obsid and (args.start or args.end):
        parser.error('Cannot use obsid argument with start or end')
    if args.cmd == 'list':
        if args.obsid:
            check_obs(subject, args.obsid)
        else:
            list_obs(subject, args.start, args.end, args.all, args.incomplete)
    elif args.cmd == 'download':
        download_artifacts(subject, args.obsid, args.threads,
                           args.start, args.end, args.retry, args.all)
    elif args.cmd == 'test':
        test(subject)


if __name__== '__main__':
    import sys
    #sys.argv = 'cadc-alma-sync list --cert /Users/adriand/.ssl/cadcproxy.pem --start 2017-01-01T01:00:00 --end 2018-12-11T01:00:00'.split()
    #sys.argv = 'cadc-alma-sync download -v --cert /Users/adriand/.ssl/cadcproxy.pem -o A001_X2f7_X492'.split()
    #sys.argv = 'cadc-alma-sync download --cert /Users/adriand/.ssl/cadcproxy.pem'.split()

    main()
