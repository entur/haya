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

package org.entur.haya.adminUnitsCache;

import org.entur.geocoder.model.GeoPoint;
import org.entur.geocoder.model.ParentType;
import org.entur.geocoder.model.Parents;
import org.entur.geocoder.model.PeliasDocument;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;

public class ParentsInfoEnricher {

    private final AdminUnitsCache adminUnitsCache;
    private final GeometryFactory geometryFactory = new GeometryFactory();

    public ParentsInfoEnricher(AdminUnitsCache adminUnitsCache) {
        this.adminUnitsCache = adminUnitsCache;
    }

    public PeliasDocument enrichParentsInfo(PeliasDocument peliasDocument) {

        if (peliasDocument.getParents() == null || peliasDocument.getSource() == null) {
            throw new IllegalArgumentException("Either Parents or Parents.source is null in the given PeliasDocument");
        }

        if (peliasDocument.getParents().isOrphan()) {
            tryAddingParentsForGivenRef(null, peliasDocument.getCenterPoint(), peliasDocument.getParents());
        } else if (peliasDocument.getParents().hasParentType(ParentType.UNKNOWN)) {
            tryAddingParentsForGivenRef(peliasDocument.getParents().idFor(ParentType.UNKNOWN), peliasDocument.getCenterPoint(), peliasDocument.getParents());
        } else if (peliasDocument.getParents().hasParentType(ParentType.LOCALITY)) {
            tryAddingParentsOfLocality(peliasDocument.getParents().idFor(ParentType.LOCALITY), peliasDocument.getCenterPoint(), peliasDocument.getParents());
        } else if (peliasDocument.getParents().hasParentType(ParentType.COUNTY)) {
            tryAddingParentsOfCounty(peliasDocument.getParents().idFor(ParentType.COUNTY), peliasDocument.getCenterPoint(), peliasDocument.getParents());
        }

        return peliasDocument;
    }

    private void tryAddingParentsForGivenRef(String ref, GeoPoint centerPoint, Parents parents) {
        if (!tryAddParentsFromCacheForGivenRef(ref, centerPoint, parents)) {
            tryAddParentsWithReverseGeoCodingForGivenCenterPoint(centerPoint, parents);
        }
    }

    private boolean tryAddParentsFromCacheForGivenRef(String ref, GeoPoint centerPoint, Parents parents) {
        AdminUnit localityFromCache = adminUnitsCache.getAdminUnitForParentType(ref, ParentType.LOCALITY);
        if (localityFromCache != null) {
            parents.addOrReplaceParent(ParentType.LOCALITY, localityFromCache.id(), localityFromCache.name());
            addParentsOfLocality(localityFromCache, parents, centerPoint);
            return true;
        }

        AdminUnit countyFromCache = adminUnitsCache.getAdminUnitForParentType(ref, ParentType.COUNTY);
        if (countyFromCache != null) {
            parents.addOrReplaceParent(ParentType.COUNTY, countyFromCache.id(), countyFromCache.name());
            tryAddingCountryWithCountryCode(countyFromCache.countryRef(), parents, centerPoint);
            return true;
        }

        AdminUnit countryFromCache = adminUnitsCache.getAdminUnitForParentType(ref, ParentType.COUNTRY);
        if (countryFromCache != null) {
            addCountryToParents(countryFromCache, parents);
            return true;
        }
        return false;
    }

    private void tryAddParentsWithReverseGeoCodingForGivenCenterPoint(GeoPoint centerPoint, Parents parents) {
        AdminUnit locality = findAdminUnitByReverseGeocoding(ParentType.LOCALITY, centerPoint);
        if (locality != null) {
            parents.addOrReplaceParent(ParentType.LOCALITY, locality.id(), locality.name());
            addParentsOfLocality(locality, parents, centerPoint);
        }

        AdminUnit county = findAdminUnitByReverseGeocoding(ParentType.COUNTY, centerPoint);
        if (county != null) {
            parents.addOrReplaceParent(ParentType.COUNTY, county.id(), county.name());
            tryAddingCountryWithCountryCode(county.countryRef(), parents, centerPoint);
        }

        AdminUnit country = findAdminUnitByReverseGeocoding(ParentType.COUNTRY, centerPoint);
        if (country != null) {
            addCountryToParents(country, parents);
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
        var point = geometryFactory.createPoint(new Coordinate(centerPoint.lon(), centerPoint.lat()));
        return switch (parentType) {
            case LOCALITY -> adminUnitsCache.getLocalityForPoint(point);
            case COUNTY -> adminUnitsCache.getCountyForPoint(point);
            case COUNTRY -> adminUnitsCache.getCountryForPoint(point);
            default -> null;
        };
    }
}