package org.entur.haya.adminUnitsCache;

import org.entur.geocoder.model.ParentType;
import org.entur.haya.peliasDocument.stopPlacestoPeliasDocument.ParentsInfoEnricher;
import org.entur.netex.index.api.NetexEntitiesIndex;
import org.locationtech.jts.geom.Point;
import org.rutebanken.netex.model.GroupOfStopPlaces;
import org.rutebanken.netex.model.IanaCountryTldEnumeration;
import org.rutebanken.netex.model.TopographicPlace;
import org.rutebanken.netex.model.ValidBetween;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public record AdminUnitsCache(Map<String, AdminUnit> countries,
                              Map<String, AdminUnit> counties,
                              Map<String, AdminUnit> localities,
                              List<GroupOfStopPlaces> groupOfStopPlaces) {

    private static final Logger logger = LoggerFactory.getLogger(AdminUnitsCache.class);

    public static AdminUnitsCache buildNewCache(NetexEntitiesIndex netexEntitiesIndex) {

        // TODO, Why groupOfStopPlaces ???
        var groupOfStopPlaces = netexEntitiesIndex.getSiteFrames().stream()
                .flatMap(siteFrame -> siteFrame.getGroupsOfStopPlaces().getGroupOfStopPlaces().stream())
                .toList();

        var allAdminUnits = netexEntitiesIndex.getSiteFrames().stream()
                .flatMap(siteFrame -> siteFrame.getTopographicPlaces().getTopographicPlace().stream())
                .filter(AdminUnitsCache::isCurrent)
                .filter(topographicPlace -> {
                    LocalDateTime toDate = topographicPlace.getValidBetween().get(0).getToDate();
                    return toDate == null || toDate.isAfter(LocalDateTime.now());
                })
                .filter(topographicPlace -> topographicPlace.getPolygon() != null)
                .map(AdminUnit::makeAdminUnit)
                .toList();

        var localities = allAdminUnits.stream()
                .filter(adminUnit -> adminUnit.adminUnitType() == AdminUnitType.LOCALITY)
                .collect(Collectors.toMap(AdminUnit::id, Function.identity()));

        var counties = allAdminUnits.stream()
                .filter(adminUnit -> adminUnit.adminUnitType() == AdminUnitType.COUNTY)
                .collect(Collectors.toMap(AdminUnit::id, Function.identity()));

        var countries = allAdminUnits.stream()
                .filter(adminUnit -> adminUnit.adminUnitType() == AdminUnitType.COUNTRY)
                .filter(adminUnit -> !adminUnit.countryRef().equals(IanaCountryTldEnumeration.RU.name()))
                .collect(Collectors.toMap(AdminUnit::id, Function.identity()));

        return new AdminUnitsCache(countries, counties, localities, groupOfStopPlaces);
    }

    private static boolean isCurrent(TopographicPlace topographicPlace) {
        ValidBetween validBetween = null;
        if (!topographicPlace.getValidBetween().isEmpty()) {
            validBetween = topographicPlace.getValidBetween().get(0);
        }
        if (validBetween == null) {
            return false;
        }
        var fromDate = validBetween.getFromDate();
        var toDate = validBetween.getToDate();
        if (fromDate != null && toDate != null && fromDate.isAfter(toDate)) {
            // Invalid Validity toDate < fromDate
            return false;
        } else return fromDate != null && toDate == null;
    }

    public AdminUnit getAdminUnitForParentType(String adminUnitId, ParentType parentType) {
        return switch (parentType) {
            case LOCALITY -> localities.get(adminUnitId);
            case COUNTY -> counties.get(adminUnitId);
            case COUNTRY -> countries.get(adminUnitId);
            default -> null;
        };
    }

    public AdminUnit getCountryForCountryRef(String countryRef) {
        AdminUnit adminUnit1 = countries.values().stream()
                .filter(country -> country.countryRef() != null)
                .filter(adminUnit -> adminUnit.countryRef().equals(countryRef))
                .findFirst().orElse(null);
        return adminUnit1;
    }

    public AdminUnit getLocalityForPoint(Point point) {
        return getAdminUnitForGivenPoint(point, localities.values());
    }

    public AdminUnit getCountyForPoint(Point point) {
        return getAdminUnitForGivenPoint(point, counties.values());
    }

    public AdminUnit getCountryForPoint(Point point) {
        return getAdminUnitForGivenPoint(point, countries.values());
    }

    private static AdminUnit getAdminUnitForGivenPoint(Point point, Collection<AdminUnit> adminUnits) {
        return adminUnits.parallelStream().filter(adminUnit -> {
            var polygon = adminUnit.geometry();
            return polygon != null && polygon.covers(point);
        }).findFirst().orElse(null);
    }
}
