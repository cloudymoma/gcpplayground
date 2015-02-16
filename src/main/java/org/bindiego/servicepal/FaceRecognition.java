package org.bindiego.servicepal;

import com.facepp.error.FaceppParseException;
import com.facepp.http.HttpRequests;
import com.facepp.http.PostParameters;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bindiego.util.Config;
import org.json.JSONObject;

import java.io.File;
import java.util.Iterator;

public class FaceRecognition {
    private static final Logger logger =
            LogManager.getFormatterLogger(FaceRecognition.class.getName());

    private PropertiesConfiguration config;

    private String apiKey;
    private String apiSecret;

    private String inputDir;
    private String outputDir;
    private String output;

    public FaceRecognition() {
        config = Config.getConfig();

        apiKey = config.getProperty("facepp.key").toString();
        apiSecret = config.getProperty("facepp.secret").toString();

        inputDir = config.getProperty("faces.inputdir").toString();
        outputDir = config.getProperty("faces.outputdir").toString();
        outputDir = config.getProperty("faces.output").toString();

        doIt();
    }

    private void doIt() {
        HttpRequests request = new HttpRequests(apiKey, apiSecret, true, true);

        int numPersons = 0;

        Iterator it = FileUtils.iterateFiles(new File(inputDir), null, false);
        while (it.hasNext()) {
            File file = (File)it.next();

            try {
                JSONObject result =
                    request.detectionDetect(
                        new PostParameters().setImg(file));

                logger.debug(result);
            } catch (FaceppParseException e) {
                logger.error("", e);
            }
        }
    }
}
