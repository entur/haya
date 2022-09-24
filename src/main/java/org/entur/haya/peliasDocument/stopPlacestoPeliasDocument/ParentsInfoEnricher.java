/*
 * Licensed under the EUPL, Version 1.2 or – as soon they will be approved by
 * the European Commission - subsequent versions of the EUPL (the "Licence");
 * You may not use this work except in compliance with the Licence.
 * You may obtain a copy of the Licence at:
 *
 *   https://joinup.ec.europa.eu/software/page/eupl
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the Licence is distributed on an "AS IS" basis,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Licence for the specific language governing permissions and
 * limitations under the Licence.
 *
 */

package org.entur.haya.peliasDocument.stopPlacestoPeliasDocument;

import org.entur.geocoder.model.GeoPoint;
import org.entur.geocoder.model.ParentType;
import org.entur.geocoder.model.Parents;
import org.entur.geocoder.model.PeliasDocument;
import org.entur.haya.adminUnitsCache.AdminUnit;
import org.entur.haya.adminUnitsCache.AdminUnitsCache;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicInteger;

public class ParentsInfoEnricher {

        private static final Logger logger = LoggerFactory.getLogger(ParentsInfoEnricher.class);

    private final AdminUnitsCache adminUnitsCache;
    private final AtomicInteger integer;
    private final GeometryFactory geometryFactory = new GeometryFactory();

    public ParentsInfoEnricher(AdminUnitsCache adminUnitsCache, AtomicInteger integer) {
        this.adminUnitsCache = adminUnitsCache;
        this.integer = integer;
    }

    public PeliasDocument enrichParentsInfo(PeliasDocument peliasDocument) {
        if (peliasDocument.getParents() == null || peliasDocument.getSource() == null) {
            logger.debug("Missing Parents or Parents.source " + peliasDocument);
            return peliasDocument;
//            throw new IllegalArgumentException("Either Parents or Parents.source is null in the given PeliasDocument");
        }

        if (peliasDocument.getParents().isOrphan()) {
            logger.debug("Orphan");
            tryAddingParentsForGivenRef(null, peliasDocument.getCenterPoint(), peliasDocument.getParents());
        } else if (peliasDocument.getParents().hasParentType(ParentType.UNKNOWN)) {
//            logger.debug("Unknown");
            tryAddingParentsForGivenRef(peliasDocument.getParents().idFor(ParentType.UNKNOWN), peliasDocument.getCenterPoint(), peliasDocument.getParents());
        } else if (peliasDocument.getParents().hasParentType(ParentType.LOCALITY)) {
            logger.debug("Locality");
            tryAddingParentsOfLocality(peliasDocument.getParents().idFor(ParentType.LOCALITY), peliasDocument.getCenterPoint(), peliasDocument.getParents());
        } else if (peliasDocument.getParents().hasParentType(ParentType.COUNTY)) {
            logger.debug("County");
            tryAddingParentsOfCounty(peliasDocument.getParents().idFor(ParentType.COUNTY), peliasDocument.getCenterPoint(), peliasDocument.getParents());
        }

        return peliasDocument;
    }

    private void tryAddingParentsForGivenRef(String ref, GeoPoint centerPoint, Parents parents) {
        // Assuming the parentType is Locality
        var locality = findAdminUnitForParentType(ParentType.LOCALITY, ref, centerPoint);

        if (locality != null) {
            parents.addOrReplaceParent(ParentType.LOCALITY, locality.id(), locality.name());
            addParentsOfLocality(locality, parents, centerPoint);
            return;
        }

        // Assuming the parentType is county
        var county = findAdminUnitForParentType(ParentType.COUNTY, ref, centerPoint);
        if (county != null) {
            parents.addOrReplaceParent(ParentType.COUNTY, county.id(), county.name());
            tryAddingCountryWithCountryCode(county.countryRef(), parents, centerPoint);
            return;
        }

        // Assuming the parentType is country
        logger.debug("its country ref " + ref);
        var country = adminUnitsCache.getAdminUnitForParentType(ref, ParentType.COUNTRY);
        if (country != null) {
            logger.debug("Found in cache");
            addCountryToParents(country, parents);
        } else {
            AdminUnit countryByReverseGeocode = findAdminUnitByReverseGeocoding(ParentType.COUNTRY, centerPoint);
            if (countryByReverseGeocode != null) {
                addCountryToParents(countryByReverseGeocode, parents);
            }
        }
    }

    private void tryAddingParentsOfLocality(String localityId, GeoPoint centerPoint, Parents parents) {
        var locality = findAdminUnitForParentType(ParentType.LOCALITY, localityId, centerPoint);
        if (locality != null) {
            addParentsOfLocality(locality, parents, centerPoint);
        }
    }

    private void addParentsOfLocality(AdminUnit locality, Parents parents, GeoPoint centerPoint) {

        var countyForLocality = findAdminUnitForParentType(ParentType.COUNTY, locality.parentId(), centerPoint);
        if (countyForLocality != null) {
            parents.addOrReplaceParent(ParentType.COUNTY, countyForLocality.id(), countyForLocality.name());
        }

        tryAddingCountryWithCountryCode(locality.countryRef(), parents, centerPoint);
    }

    private void tryAddingParentsOfCounty(String countyId, GeoPoint centerPoint, Parents parents) {
        var county = findAdminUnitForParentType(ParentType.COUNTY, countyId, centerPoint);
        if (county != null) {
            tryAddingCountryWithCountryCode(county.countryRef(), parents, centerPoint);
        }
    }

    private void tryAddingCountryWithCountryCode(String countryCode, Parents parents, GeoPoint centerPoint) {
        if (countryCode.equals("NO")) {
            // TODO: Remove this when Assad adds Norway as TopographicPlace i NSR netex file.
            parents.addOrReplaceParent(ParentType.COUNTRY, "FAKE-ID", "Norway", "NOR");
            return;
        }
        var country = findAdminUnitForParentType(ParentType.COUNTRY, countryCode, centerPoint);
        addCountryToParents(country, parents);
    }

    private void addCountryToParents(AdminUnit country, Parents parents) {
        if (country != null) {
            parents.addOrReplaceParent(
                    ParentType.COUNTRY,
                    country.id(),
                    country.name(),
                    country.getISO3CountryName());
        }
    }

    private AdminUnit findAdminUnitForParentType(ParentType parentType, String ref, GeoPoint centerPoint) {
        // Try getting parent info by id.
        var adminUnit = switch (parentType) {
            case LOCALITY, COUNTY -> ref != null ? adminUnitsCache.getAdminUnitForParentType(ref, parentType) : null;
            case COUNTRY -> ref != null ? adminUnitsCache.getCountryForCountryRef(ref) : null;
            default -> null;
        };

        // Try getting parent info by reverse geocoding.
        return adminUnit != null ? adminUnit : findAdminUnitByReverseGeocoding(parentType, centerPoint);
    }

    private AdminUnit findAdminUnitByReverseGeocoding(ParentType parentType, GeoPoint centerPoint) {
        logger.debug("Called for: " + parentType + " " + integer.incrementAndGet());
        var point = geometryFactory.createPoint(new Coordinate(centerPoint.lon(), centerPoint.lat()));

        AdminUnit adminUnit = switch (parentType) {
            case LOCALITY -> adminUnitsCache.getLocalityForPoint(point);
            case COUNTY -> adminUnitsCache.getCountyForPoint(point);
            case COUNTRY -> adminUnitsCache.getCountryForPoint(point);
            default -> null;
        };

        logger.debug("Found " + parentType + ": " + (adminUnit != null));

        return adminUnit;
    }
}