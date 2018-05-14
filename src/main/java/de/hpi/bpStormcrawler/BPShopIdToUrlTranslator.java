package de.hpi.bpStormcrawler;

import com.digitalpebble.stormcrawler.util.ConfUtils;
import com.google.api.client.auth.oauth.OAuthGetAccessToken;
import com.google.api.client.auth.oauth.OAuthParameters;
import com.google.api.client.auth.oauth2.*;
import com.google.api.client.http.*;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.JsonGenerator;
import com.google.api.client.json.JsonParser;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

import java.io.*;
import java.nio.charset.Charset;
import java.util.Collections;
import java.util.Map;

@Getter(AccessLevel.PRIVATE)
@Setter(AccessLevel.PRIVATE)

public class BPShopIdToUrlTranslator extends BaseRichBolt{
    private String clientSecret;
    private String clientId;
    private String accessTokenUri;
    private String apiUrl;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        clientSecret = ConfUtils.getString(map, "oauth.clientSecret", "");
        clientId = ConfUtils.getString(map, "oauth.clientId", "");
        accessTokenUri = ConfUtils.getString(map, "oauth.accessTokenUri", "");
        apiUrl = ConfUtils.getString(map, "oauth.apiUrl", "");
    }

    @Override
    public void execute(Tuple tuple) {

        //TODO Take JacksonFactory here
        JsonFactory jsonFactory = new JsonFactory() {
            @Override
            public JsonParser createJsonParser(InputStream inputStream) throws IOException {
                return null;
            }

            @Override
            public JsonParser createJsonParser(InputStream inputStream, Charset charset) throws IOException {
                return null;
            }

            @Override
            public JsonParser createJsonParser(String s) throws IOException {
                return null;
            }

            @Override
            public JsonParser createJsonParser(Reader reader) throws IOException {
                return null;
            }

            @Override
            public JsonGenerator createJsonGenerator(OutputStream outputStream, Charset charset) throws IOException {
                return null;
            }

            @Override
            public JsonGenerator createJsonGenerator(Writer writer) throws IOException {
                return null;
            }
        };


        HttpTransport httpTransport = new NetHttpTransport();

        AuthorizationCodeFlow flow = new AuthorizationCodeFlow.Builder(
                BearerToken.authorizationHeaderAccessMethod(),
                httpTransport, jsonFactory,
                new GenericUrl(getAccessTokenUri()),
                new ClientParametersAuthentication(getClientId(),getClientSecret()),
                /* Client ID */
                getApiUrl(), getApiUrl()).build();
        //TODO Fix AuthorizationCodeFlow

        //TODO Implement Token Response
        // https://blog.takipi.com/tutorial-how-to-implement-java-oauth-2-0-to-sign-in-with-github-and-google/
        try {
            TokenResponse tokenResponse = flow
                    .newTokenRequest(" ")
                    .setScopes(Collections.singletonList("user:email"))
                    .setRequestInitializer(new HttpRequestInitializer() {
                        @Override
                        public void initialize(HttpRequest request) throws IOException {
                            request.getHeaders().setAccept("application/json");
                        }
                    }).execute();
        } catch (IOException e) {
            e.printStackTrace();
        }



    }

    public static HttpResponse executeGet(
            HttpTransport transport, JsonFactory jsonFactory, String accessToken, GenericUrl url)
            throws IOException {
        Credential credential =
                new Credential(BearerToken.authorizationHeaderAccessMethod()).setAccessToken(accessToken);
        HttpRequestFactory requestFactory = transport.createRequestFactory(credential);
        return requestFactory.buildGetRequest(url).execute();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("url", "metadata", "status"));
    }
}
