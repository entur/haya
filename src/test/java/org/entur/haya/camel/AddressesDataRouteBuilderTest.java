package org.entur.haya.camel;

import org.apache.camel.EndpointInject;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.RoutesBuilder;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.mock.MockEndpoint;
import org.apache.camel.test.spring.junit5.CamelSpringBootTest;
import org.apache.commons.io.IOUtils;
import org.entur.geocoder.blobStore.BlobStoreRepository;
import org.entur.haya.HayaApplication;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.util.ReflectionTestUtils;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@CamelSpringBootTest
@EnableAutoConfiguration
@SpringBootTest(
        properties = {"camel.springboot.name=customName"},
        classes = HayaApplication.class
)
public class AddressesDataRouteBuilderTest {

    @Autowired
    AddressesDataRouteBuilder addressesDataRouteBuilder;

    @Autowired
    ProducerTemplate producerTemplate;

    @EndpointInject("mock:makeCSV")
    MockEndpoint mockEndpoint;

    @MockBean
    BlobStoreRepository blobStoreRepository;

    @TempDir
    Path workingDirectory;

    //Spring context fixtures
    @Configuration
    static class TestConfig {

        @Bean
        RoutesBuilder route() {
            return new RouteBuilder() {
                @Override
                public void configure() throws Exception {
                    from("direct:test").to("mock:test");
                }
            };
        }
    }

    @Test
    public void shouldAutowireProducerTemplate() {
        assertNotNull(producerTemplate);
    }

    @Test
    public void shouldSetCustomName() {
        assertEquals("customName", producerTemplate.getCamelContext().getName());
    }

    @Test
    public void shouldInjectEndpoint() throws InterruptedException, IOException {

        ReflectionTestUtils.setField(addressesDataRouteBuilder, "hayaWorkDir", workingDirectory.toString());

        InputStream adminUnits = this.getClass().getResourceAsStream("/org/entur/haya/camel/tiamat_export_geocoder_latest.zip");
        InputStream addresses = this.getClass().getResourceAsStream("/org/entur/haya/camel/Basisdata_0000_Norge_25833_MatrikkelenAdresse_CSV.zip");
        InputStream result = this.getClass().getResourceAsStream("/org/entur/haya/camel/haya_export_geocoder_1663155120267.zip");

        ArgumentCaptor<String> keyCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<ByteArrayInputStream> valueCaptor = ArgumentCaptor.forClass(ByteArrayInputStream.class);

        when(blobStoreRepository.getBlob(any())).thenReturn(adminUnits);
        when(blobStoreRepository.getLatestBlob(any())).thenReturn(addresses);

//        mockEndpoint.setExpectedMessageCount(1);
        producerTemplate.sendBody("direct:makeCSV", "msg");

//        mockEndpoint.assertIsSatisfied();



        verify(blobStoreRepository, times(2)).uploadBlob(keyCaptor.capture(), valueCaptor.capture());


        String csvFileName = keyCaptor.getAllValues().get(0);
        String currentFileName = keyCaptor.getAllValues().get(1);

        ByteArrayInputStream csvFileInputStream = valueCaptor.getAllValues().get(0);
        ByteArrayInputStream currentFileInputStream = valueCaptor.getAllValues().get(1);

        assertTrue(IOUtils.contentEquals(csvFileInputStream, result));
        assertEquals(csvFileName, new String(currentFileInputStream.readAllBytes(), StandardCharsets.UTF_8));

/*        verify(blobStoreRepository, times(2)).uploadBlob(argThat((arg) -> {
                    return true;
                }),
                argThat((arg) -> {
                    try {
                        return IOUtils.contentEquals(result, arg);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }));
*/
    }
}