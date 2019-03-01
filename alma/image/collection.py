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

# collection specific code to return a list of observation IDs or a
# specific CAOM2 observation

from datetime import datetime
import requests
import mimetypes
from astroquery.alma import Alma
from caom2.shape import SubInterval
import caom2
from cadcutils.util import date2ivoa
from six import BytesIO
import logging
from astropy import units as u
from astropy import constants as const
from astropy.time import Time as AstropyTime
import re
from astropy.io.votable import parse_single_table
import copy


COLLECTION = 'ALMA'

ALMA_DATE_FORMAT = '%Y-%m-%d %H:%M:%S'
ALMA_QUERY_DATE_FORMAT = '%d-%m-%Y'
ALMA_RELEASE_DATE_FORMAT = '%Y-%m-%d'

ALMA_TAP_SYNC_URL = 'https://almascience.nrao.edu/tap/sync'


logger = logging.getLogger('caom2proxy')
logger.setLevel(logging.DEBUG)

now = datetime.utcnow()

# This files overrides the base functionality provided in the
# collection.py file of the proxy base docker image


def list_observations(start=None, end=None, maxrec=None):
    """
    List observations based on their observation dates. It implements the
    functionality required by the base proxy docker image for listing ALMA
    observations
    :param start: start observation date (UTC)
    :param end: end observation date (UTC)
    :param maxrec: maximum number of rows to return
    :return: Comma separated list, each row consisting of ObservationID,
    observation date.
    """

    where = ''
    if start or end:
        if start:
            where = 'WHERE t_min>={}'.format(AstropyTime(start).mjd)
            if end:
                where += ' AND t_min<={}'.format(AstropyTime(end).mjd)
        else:
            where = 'WHERE t_min<={}'.format(AstropyTime(end).mjd)
    top = ''

    if maxrec:
        if int(maxrec) < 1:
            raise AttributeError('maxrec must be positive integer')
        top = 'TOP {}'.format(maxrec)
    query = "SELECT {} obs_id AS observationID, min(t_min) AS obsTime " \
            "FROM alma.obscore {} GROUP BY obs_id ORDER by obsTime".\
        format(top, where)
    response = requests.get(ALMA_TAP_SYNC_URL,
                            params={'QUERY': query, 'LANG': 'ADQL'})
    response.raise_for_status()
    temp = BytesIO(response.content)
    obs_ids = parse_single_table(temp)

    result = []
    for r in obs_ids.array:
        obsID = _to_obs_id(r[0].decode('ascii'))
        timestamp = date2ivoa(AstropyTime(r[1], format='mjd').datetime)
        result.append('{}\t{}\t{}\n'.format(COLLECTION, obsID, timestamp))

    return result


def get_observation(id):
    """
    Return the observation corresponding to the id. It is the ALMA
    implementation of the get_observation function expected by the base
    proxy alma docker image.
    :param id: id of the observation
    :return: observation corresponding to the id or None if such
    such observation does not exist
    """
    member_ouss_id = _to_member_ouss_id(id)
    # alternative ALMA mirror
    # Alma.archive_url = 'http://almascience.eso.org'
    results = Alma().query({'member_ous_id': member_ouss_id}, science=False,
                           cache=False, format='ascii')

    if not results:
        logger.debug('No observation found for ID : {}'.format(member_ouss_id))
        return None
    return member2observation(member_ouss_id, results)


def _to_obs_id(member_ouss_id):
    # transformation needed to make obs id compatible with the path of
    # a URI as required by the CAOM2 model
    return member_ouss_id.replace('uid://', '').replace('/', '_')


def _to_member_ouss_id(obs_id):
    # reverse transformation
    return 'uid://{}'.format(obs_id.replace('_', '/'))


# Code for proxy caom2 service
def member2observation(member_ous, table):
    """
    Turns the ALMA metadata corresponding to a member ous into
    a CAOM2 observation.
    """
    observationID = _to_obs_id(member_ouss_id=member_ous)
    logger.debug('observationID = {}'.format(observationID))
    observation = caom2.SimpleObservation('ALMA', observationID)
    cal_planes = get_calib_planes(observation, table)

    # observation metadata is common amongst rows so get it from the first
    # row
    fr = table[0]
    observation.meta_release = \
        datetime.strptime(fr['Observation date'].decode('ascii'),
                          ALMA_DATE_FORMAT)
    add_raw_plane(observation, cal_planes,
                  member_ous, observation.meta_release)
    proposal = caom2.Proposal(fr['ASA_PROJECT_CODE'])
    proposal.pi_name = fr['PI name']
    proposal.title = fr['Project title']
    proposal.keywords = set(fr['Science keyword'].split(','))
    observation.proposal = proposal
    instrument = caom2.Instrument('Band {}'.format(fr['Band'][0]))
    observation.instrument = instrument
    observation.algorithm = caom2.Algorithm('Exposure')
    observation.intent = caom2.ObservationIntentType.SCIENCE
    observation.telescope = \
        caom2.Telescope('ALMA-{}'.format(fr['Array'].decode('ascii')),
                        2225142.18, -5440307.37, -2481029.852)

    # environment info
    env = caom2.Environment()
    env.tau = fr['PWV']/0.935 + 0.35
    env.wavelength_tau = 350*u.um.to(u.meter)
    observation.environment = env
    target_name = _get_obs_target_name(table)
    if target_name:
        observation.target = caom2.Target(name=target_name,
                                          target_type=caom2.TargetType.OBJECT)
    observation.last_modified = now
    observation.max_last_modified = now
    return observation


def get_calib_planes(observation, table):
    calib_planes = []
    target_planes = 0  # number of target planes
    for row in table:
        temp = row['Scan intent'].lower().replace(' ', '')
        product_id = '{}-{}'.format(observation.observation_id, temp)
        if 'target' == temp:
            target_planes += 1
            product_id += str(target_planes)
            target_plane = True
        else:
            target_plane = False
        plane = caom2.Plane(product_id)
        meta_release = \
            datetime.strptime(row['Observation date'].decode('ascii'),
                              ALMA_DATE_FORMAT)
        if 'Observation date' in row:
            meta_release = datetime.strptime(
                row['Observation date'].decode('ascii'), ALMA_DATE_FORMAT)
        plane.meta_release = meta_release
        tmp = row['Release date']
        try:
            if isinstance(tmp, bytes):
                tmp = tmp.decode('ascii')
            plane.data_release = datetime.strptime(tmp,
                                                   ALMA_RELEASE_DATE_FORMAT)
        except Exception as f:
            msg = 'Observation {} - no valid release date: {}.'.\
                format(observation.observation_id, tmp)
            logger.debug(msg)
            logger.debug(f)
            raise RuntimeError(msg)
        if plane.data_release > datetime.utcnow():
            raise RuntimeError(
                'Observation {} is proprietary. Release date: {}.'.
                format(observation.observation_id, plane.data_release))

        plane.position = _get_position(row, table)
        plane.energy = _get_energy(row, table)
        plane.time = _get_time(row, table)
        plane.polarization = _get_polarization(row)

        if target_plane:
            plane.data_product_type = caom2.DataProductType.VISIBILITY
        else:
            plane.data_product_type = caom2.DataProductType.CUBE
        plane.calibration_level = caom2.CalibrationLevel.CALIBRATED
        plane.last_modified = now
        plane.max_last_modified = now
        calib_planes.append(plane)
    return calib_planes


def add_raw_plane(observation, cal_planes, member_ous, meta_release):
    """
    Adds raw plane to observation
    NOTE: this is to be called after the calibration planes have been added
    """
    productID = observation.observation_id + '-raw'
    plane = caom2.Plane(productID)
    plane.artifacts = caom2.TypedOrderedDict(caom2.Artifact)
    plane.meta_release = meta_release
    plane.calibration_level = caom2.CalibrationLevel.RAW_INSTRUMENTAL
    # wcs is shared with the target plane except position which
    # could consist in multiple disjoint areas that cannot be accurately
    # represented. Exposure is the sum of all the exposures in the other planes
    exposure = 0
    done = False
    for cp in cal_planes:
        exposure += cp.time.exposure
        if not done and 'target' in cp.product_id:
            plane.energy = cp.energy
            # need to make a copy in order to update the exposure later
            plane.time = copy.deepcopy(cp.time)
            plane.polarization = cp.polarization
            done = True
    plane.time.exposure = exposure
    plane.data_product_type = caom2.DataProductType.VISIBILITY
    plane.last_modified = now
    plane.max_last_modified = now
    observation.planes[productID] = plane
    add_raw_artifacts(plane, member_ous)


def add_raw_artifacts(plane, member_ous):
    """ Adds all the raw artifacts to the plane """
    files = Alma().stage_data(member_ous)
    file_urls = list(set(files['URL']))
    print('\n'.join(file_urls))
    for file_url in file_urls:
        add_raw_artifact(plane, file_url)


def add_raw_artifact(plane, file_url):
    """ Adds a raw artifact to the plane """
    filename = file_url.split('/')[-1]
    file_uri = 'alma:ALMA/' + filename
    file_header = requests.head(file_url)
    content_type = file_header.headers['Content-Type']
    if content_type == '':
        content_type = mimetypes.guess_type(file_url)[0]
    product_type = caom2.ProductType.SCIENCE
    artifact = caom2.Artifact(file_uri, product_type,
                              caom2.ReleaseType.META,
                              content_type=content_type)
    try:
        artifact.content_length = int(file_header.headers['Content-Length'])
    except KeyError:
        # not content length returned
        pass
    artifact.last_modified = now
    artifact.max_last_modified = now
    plane.artifacts[file_uri] = artifact


def _get_position(row, table):
    # Extracts position from a returned row of the ALMA results table
    position = caom2.Position()
    position.resolution = row['Spatial resolution']
    # Shape is circle
    # make sure all units are degrees
    ra = row['RA'] * table['RA'].unit.to(u.deg)
    dec = row['Dec'] * table['Dec'].unit.to(u.deg)
    radius = row['Field of view'] * table['Field of view'].unit.to(u.deg) / 2.0
    circle = caom2.Circle(caom2.Point(ra, dec), radius)
    position.bounds = circle
    return position


def _get_energy(row, table):
    # Extracts the energy inform from a returned row
    min_bound = None
    max_bound = None
    si = []  # list of non-overlapping sub-intervals
    for b in re.findall(r'\[([^]]*)\]', row['Frequency support']):
        e_int = b.split(',')[0]
        vals = e_int.split('..')
        lower_freq = float(vals[0])
        # upper string of form: 123.45GHz
        upper_str = re.findall(r'\b\d+\.?\d+', vals[1])[0]
        upper_freq = float(upper_str)
        units = u.Unit(vals[1][len(upper_str):])
        # wavelengths in meters:
        upper = \
            (lower_freq*units).to(u.meter, equivalencies=u.spectral()).value
        lower = \
            (upper_freq*units).to(u.meter, equivalencies=u.spectral()).value
        si = _add_subinterval(si, (lower, upper))
        if min_bound is not None:
            min_bound = min(min_bound, lower)
        else:
            min_bound = lower
        if max_bound is not None:
            max_bound = max(max_bound, upper)
        else:
            max_bound = upper
    samples = []
    for s in si:
        samples.append(SubInterval(s[0], s[1]))
    bounds = caom2.Interval(min_bound, max_bound, samples=samples)
    resolving_power = \
        const.c/row['Velocity resolution']*table['Velocity resolution'].\
        unit.to(u.m/u.s)
    return caom2.Energy(bounds=bounds, resolving_power=resolving_power.value,
                        em_band=caom2.EnergyBand.MILLIMETER)


def _add_subinterval(si_list, subinterval):
    # Adds and interval to a list of intervals eliminating (merging) any
    # overlaps

    if not si_list:
        return [subinterval]
    # check for overlaps
    # begining of the list?
    if subinterval[1] < si_list[0][0]:
        return [subinterval] + si_list
    if subinterval[0] > si_list[-1][1]:
        return si_list + [subinterval]
    result = []
    for si in si_list:
        if (si[0] >= subinterval[0] and si[0] <= subinterval[1]) or\
           (subinterval[0] >= si[0] and subinterval[0] <= si[1]):
            # overlap detected
            subinterval = (min(si[0], subinterval[0]),
                           max(si[1], subinterval[1]))
        else:
            if subinterval[0] < si[0]:
                result += [subinterval]
                result += si_list[si_list.index(si):]
                return result
            else:
                result += [si]
    return result + [subinterval]


def _get_time(row, table):
    # Extracts time information from a rrow fo returned ALMA table
    time = caom2.Time()
    time.exposure = \
        row['Integration'] * table['Integration'].unit.to(u.second)
    time_lb = AstropyTime(datetime.strptime(
        row['Observation date'].decode('ascii'), ALMA_DATE_FORMAT),
        scale='utc')
    time_ub = time_lb + time.exposure * u.second
    time_interval = caom2.Interval(time_lb.mjd, time_ub.mjd)
    samples = SubInterval(time_lb.mjd, time_ub.mjd)
    time_interval.samples = [samples]
    time.bounds = time_interval
    return time


def _get_polarization(row):
    # Extracts polarization information from a row
    polarization = caom2.Polarization()
    polarization.polarization_states = \
        [caom2.PolarizationState(i) for i in row['Pol products'].split()]
    return polarization


def _get_obs_target_name(table):
    # The target name is given by the "source name" of a target plane/row.
    # If the observation has multiple target planes, target name is ''
    # TODO A proposal has been made to push the "Target name" to the plane
    # level in which case each plane will have on corresponding to the
    # "Source name"
    target_name = ''
    for row in table:
        if row['Scan intent'].lower() == 'target':
            if target_name and target_name != row['Source name']:
                # different target names, return empty string as we can't tell
                return None
            else:
                target_name = row['Source name']
    return target_name
