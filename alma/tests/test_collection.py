# -*- coding: utf-8 -*-
# ***********************************************************************
# ******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
# *************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
#
#  (c) 2018.                            (c) 2018.
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


import sys
from datetime import datetime
from caom2utils import caomvalidator
from caom2 import obs_reader_writer, get_differences
from cadcutils.util import IVOA_DATE_FORMAT
import tempfile
import os
import pytest
import re

one_test = False

PARENT_DIR = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
IMAGE_DIR = os.path.join(PARENT_DIR, 'image')
sys.path.insert(0, IMAGE_DIR)

# following imports need to occur after we've added collection.py to the path
import collection  # noqa
from collection import _add_subinterval  # noqa

DATA_DIR = os.path.join(PARENT_DIR, 'tests', 'data')

ALMA_OBS_IDS = ['A001_X87d_X8bc', 'A001_X11a2_X11', 'A001_X144_Xef',
                'A001_X131c_X11f']
ALMA_PROPRIETARY_OBS_IDS = ['A001_X2cf_X15']


@pytest.mark.skipif(one_test, reason='One test mode')
def test_subintervals():
    assert _add_subinterval([], (2, 6)) == [(2, 6)]
    assert _add_subinterval([(7, 10)], (2, 6)) == [(2, 6), (7, 10)]
    assert _add_subinterval([(2, 6)], (7, 10)) == [(2, 6), (7, 10)]
    assert _add_subinterval([(2, 6)], (2, 6)) == [(2, 6)]
    assert _add_subinterval([(2, 6)], (3, 5)) == [(2, 6)]
    assert _add_subinterval([(7, 10)], (2, 8)) == [(2, 10)]
    assert _add_subinterval([(2, 3), (6, 7)], (4, 5)) == \
        [(2, 3), (4, 5), (6, 7)]
    assert _add_subinterval([(2, 3), (4, 5), (6, 7)], (4, 8)) == \
        [(2, 3), (4, 8)]
    assert _add_subinterval([(2, 3), (4, 5), (6, 7)], (2, 7)) == [(2, 7)]


@pytest.mark.skipif(one_test, reason='One test mode')
def test_resolve_artifact():
    url = collection.resolve_artifact_uri(
        'alma:ALMA/2017.A.00054.S_uid___A001_X131c_X12f_auxiliary.tar')
    assert re.match(
        'http.*2017.A.00054.S_uid___A001_X131c_X12f_auxiliary.tar', url)
    # the other url of the other artifact in the plane is cached


@pytest.mark.skipif(one_test, reason='One test mode')
def test_get_obs():
    obs = collection.list_observations(start=datetime.strptime('01-03-2011',
                                                               '%d-%m-%Y'),
                                       end=datetime.utcnow(), maxrec=10)
    assert 10 == len(obs)
    obs = collection.list_observations(start=datetime.strptime('01-03-2011',
                                                               '%d-%m-%Y'),
                                       end=datetime.strptime('01-04-2011',
                                                             '%d-%m-%Y'))
    for i, o in enumerate(obs):
        fields = o.split('\t')
        assert 'ALMA' == fields[0].strip()


@pytest.mark.skipif(one_test, reason='One test mode')
def test_get_observation():
    """
    NOTE: This hits the ALMA service and it will only work with Internet
    connection and the ALMA service being up and running.
    :return:
    """
    # access ALMA to get a set of observations and verifies them
    dir = tempfile.mkdtemp()
    obs_file = os.path.join(dir, 'obs.xml')

    writer = obs_reader_writer.ObservationWriter(validate=True)
    reader = obs_reader_writer.ObservationReader()
    for id in ALMA_OBS_IDS:
        obs = collection.get_observation(id)
        caomvalidator.validate(obs)
        # write it to a temporary file to make sure it passes the xml
        # validation too
        writer.write(obs, obs_file)
        benchmark_obs_file = os.path.join(DATA_DIR, '{}.xml'.format(id))
        # An easy way to update the benchmark files when the mapping algorithm
        # is updated. Briefly uncomment the following line to update the
        # files and comment it back after.
        # writer.write(obs, benchmark_obs_file)

        # compare with what we are expecting
        expected_obs = reader.read(benchmark_obs_file)
        assert not get_differences(expected_obs, obs)

    # check that a RuntimeError with status_code 403 is thrown when trying to
    # get a proprietary observation. Hopefully the used observation will stay
    # proprietary until the release date in year 3000 ...
    for id in ALMA_PROPRIETARY_OBS_IDS:
        with pytest.raises(RuntimeError) as e_info:
            collection.get_observation(id)
    assert 403 == e_info.value.status_code
