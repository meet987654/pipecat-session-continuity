"""Raw PCM frame serializer for Pipecat.

Bridges a plain browser WebSocket (which sends/receives raw Int16 PCM bytes)
to Pipecat's FastAPIWebsocketTransport, which requires a FrameSerializer.
"""

from pipecat.frames.frames import (
    Frame,
    InputAudioRawFrame,
    OutputAudioRawFrame,
)
from pipecat.serializers.base_serializer import FrameSerializer


class RawPCMSerializer(FrameSerializer):
    """Serializer that treats every binary message as raw PCM Int16 audio.

    - deserialize: wraps incoming bytes into an InputAudioRawFrame
    - serialize:   extracts raw audio bytes from OutputAudioRawFrame
    """

    def __init__(self, sample_rate: int = 16000, num_channels: int = 1):
        super().__init__()
        self._sample_rate = sample_rate
        self._num_channels = num_channels

    async def serialize(self, frame: Frame) -> str | bytes | None:
        if isinstance(frame, OutputAudioRawFrame):
            return frame.audio
        return None

    async def deserialize(self, data: str | bytes) -> Frame | None:
        if isinstance(data, bytes):
            return InputAudioRawFrame(
                audio=data,
                sample_rate=self._sample_rate,
                num_channels=self._num_channels,
            )
        return None
