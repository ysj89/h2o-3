package water.api.schemas3;

import hex.mli.CorrelationFrame;
import water.api.API;
import water.api.schemas3.KeyV3.FrameKeyV3;

public class CorrelationFrameV3 extends RequestSchemaV3<CorrelationFrame,CorrelationFrameV3> {
    @API(help="input frame x", direction = API.Direction.INPUT)
    public FrameKeyV3 frame_x_id;

    @API(help="input frame y", direction = API.Direction.INPUT)
    public FrameKeyV3 frame_y_id;

    @API(help="use", direction = API.Direction.INPUT)
    public String use;

    @API(help="output frame", direction=API.Direction.OUTPUT)
    public FrameKeyV3 result;
}
