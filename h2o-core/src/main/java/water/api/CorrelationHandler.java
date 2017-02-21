package water.api;

import water.DKV;
import water.Key;
import water.api.schemas3.CorrelationFrameV3;
import water.api.schemas3.KeyV3;
import water.fvec.Frame;
import hex.mli.CorrelationFrame;

public class CorrelationHandler extends Handler {

    public CorrelationFrameV3 getCorrelationFrame(int version, CorrelationFrameV3 corrFrame) {
        Frame fr = FramesHandler.getFromDKV("key", corrFrame.frame_x_id.key());
        Frame fr2 = FramesHandler.getFromDKV("key",corrFrame.frame_y_id.key());
        String use = corrFrame.use;
        Frame corr = CorrelationFrame.correlationFrame(fr,fr2,use);
        corr._key = Key.make("cor" + "_" + fr._key + "_" + fr2._key + "_" + use);
        DKV.put(corr._key,corr);
        corrFrame.result = new KeyV3.FrameKeyV3(corr._key);
        return corrFrame;
    }

}