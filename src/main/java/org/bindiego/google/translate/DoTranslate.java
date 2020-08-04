package org.bindiego.google.translate;

import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bindiego.util.Config;

import org.json.JSONObject;
import com.google.cloud.translate.Translate;
import com.google.cloud.translate.Translate.TranslateOption;
import com.google.cloud.translate.Translation;

class DoTranslate implements Runnable {
    private DoTranslate() {}

    public DoTranslate(final Translate trans, final String sourceLang, final String txt) {
        // Instantiate or get the current Global config
        config = Config.getConfig();

        this.trans = trans;
        this.txt = txt;
        this.sourceLang = sourceLang;
    }

    @Override
    public void run() {
        // loop control
        int numLoops = Integer.parseInt(
            config.getProperty("google.translate.thread.requests").toString());

        Translation translation = null;

        for (int i = 0; i < numLoops; ++i) {
            try {
                translation = trans.translate(
                    txt,
                    TranslateOption.sourceLanguage(sourceLang),
                    TranslateOption.targetLanguage(
                        config.getProperty("google.translate.target").toString()));

                logger.info("Translated text: %s", translation.getTranslatedText());
            } catch (Exception ex) {
                logger.error("BOOM!", ex);
                System.exit(-2);
            }
        }
    }

    private static final Logger logger =
        LogManager.getFormatterLogger(DoTranslate.class.getName());

    private PropertiesConfiguration config;

    private Translate trans;
    private String sourceLang;
    private String txt;
}
