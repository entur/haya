package org.entur.haya.adminUnitsCache;

import org.entur.geocoder.model.*;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;

public class ParentsInfoEnricher {

    private final AdminUnitsCache adminUnitsCache;
    private final GeometryFactory geometryFactory = new GeometryFactory();

    public ParentsInfoEnricher(AdminUnitsCache adminUnitsCache) {
        this.adminUnitsCache = adminUnitsCache;
    }

    public PeliasDocument enrichParentsInfo(PeliasDocument peliasDocument) {

        // TODO , why this check??
        if (peliasDocument.getParents() == null) {
            throw new IllegalArgumentException("Parents is null in the given PeliasDocument");
        }

        if (peliasDocument.getParents().isOrphan()) {
            tryAddingParentsForGivenId(null, peliasDocument.getCenterPoint(), peliasDocument.getParents());
        } else if (peliasDocument.getParents().hasParentType(ParentType.UNKNOWN)) {
            tryAddingParentsForGivenId(peliasDocument.getParents().idFor(ParentType.UNKNOWN), peliasDocument.getCenterPoint(), peliasDocument.getParents());
        } else if (peliasDocument.getParents().hasParentType(ParentType.LOCALITY)) {
            tryAddingParentsOfLocality(peliasDocument.getParents().idFor(ParentType.LOCALITY), peliasDocument.getCenterPoint(), peliasDocument.getParents());
        } else if (peliasDocument.getParents().hasParentType(ParentType.COUNTY)) {
            tryAddingParentsOfCounty(peliasDocument.getParents().idFor(ParentType.COUNTY), peliasDocument.getCenterPoint(), peliasDocument.getParents());
        }

        return peliasDocument;
    }

    private void tryAddingParentsForGivenId(PeliasId id, GeoPoint centerPoint, Parents parents) {
        if (!tryAddParentsFromCacheForGivenId(id, centerPoint, parents)) {
            tryAddParentsWithReverseGeoCodingForGivenCenterPoint(centerPoint, parents);
        }
    }

    private boolean tryAddParentsFromCacheForGivenId(PeliasId id, GeoPoint centerPoint, Parents parents) {
        AdminUnit localityFromCache = adminUnitsCache.getAdminUnitForParentType(id, ParentType.LOCALITY);
        if (localityFromCache != null) {
            parents.addOrReplaceParent(ParentType.LOCALITY, localityFromCache.id(), localityFromCache.name());
            addParentsOfLocality(localityFromCache, parents, centerPoint);
            return true;
        }

        AdminUnit countyFromCache = adminUnitsCache.getAdminUnitForParentType(id, ParentType.COUNTY);
        if (countyFromCache != null) {
            parents.addOrReplaceParent(ParentType.COUNTY, countyFromCache.id(), countyFromCache.name());
            tryAddingCountryWithCountryCode(countyFromCache.countryRef(), parents, centerPoint);
            return true;
        }

        AdminUnit countryFromCache = adminUnitsCache.getAdminUnitForParentType(id, ParentType.COUNTRY);
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

    private void tryAddingParentsOfLocality(PeliasId localityId, GeoPoint centerPoint, Parents parents) {
        var locality = findAdminUnitForPeliasId(ParentType.LOCALITY, localityId, centerPoint);
        if (locality != null) {
            addParentsOfLocality(locality, parents, centerPoint);
        }
    }

    private void addParentsOfLocality(AdminUnit locality, Parents parents, GeoPoint centerPoint) {

        var countyForLocality = findAdminUnitForPeliasId(ParentType.COUNTY, locality.parentId(), centerPoint);
        if (countyForLocality != null) {
            parents.addOrReplaceParent(ParentType.COUNTY, countyForLocality.id(), countyForLocality.name());
        }

        tryAddingCountryWithCountryCode(locality.countryRef(), parents, centerPoint);
    }

    private void tryAddingParentsOfCounty(PeliasId countyId, GeoPoint centerPoint, Parents parents) {
        var county = findAdminUnitForPeliasId(ParentType.COUNTY, countyId, centerPoint);
        if (county != null) {
            tryAddingCountryWithCountryCode(county.countryRef(), parents, centerPoint);
        }
    }

    private void tryAddingCountryWithCountryCode(String countryCode, Parents parents, GeoPoint centerPoint) {
        if (countryCode.equals("NO")) {
            // TODO: Remove this when Assad adds Norway as TopographicPlace i NSR netex file.
            parents.addOrReplaceParent(
                    ParentType.COUNTRY,
                    new PeliasId("KVE", "TopographicPlace", "Norway"),
                    "Norway",
                    "NOR");
            return;
        }
        var country = findAdminUnitForCountryCode(countryCode, centerPoint);
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

    private AdminUnit findAdminUnitForPeliasId(ParentType parentType, PeliasId id, GeoPoint centerPoint) {
        // Try getting parent info by id.
        var adminUnit = id != null ? adminUnitsCache.getAdminUnitForParentType(id, parentType) : null;
        // Try getting parent info by reverse geocoding.
        return adminUnit != null ? adminUnit : findAdminUnitByReverseGeocoding(parentType, centerPoint);
    }

    private AdminUnit findAdminUnitForCountryCode(String countryCode, GeoPoint centerPoint) {
        // Try getting parent info by id.
        var adminUnit = countryCode != null ? adminUnitsCache.getCountryForCountryRef(countryCode) : null;

        // Try getting parent info by reverse geocoding.
        return adminUnit != null ? adminUnit : findAdminUnitByReverseGeocoding(ParentType.COUNTRY, centerPoint);
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