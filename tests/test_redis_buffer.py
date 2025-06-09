# tests/test_redis_buffer.py
import pytest
import json
from datetime import datetime, timedelta
from app.services.redis_buffer import RedisBuffer
from app.core.config import get_settings


@pytest.fixture
async def redis_buffer():
    settings = get_settings()
    buffer = RedisBuffer(redis_url=settings.redis_url, environment="test")
    await buffer.connect()
    yield buffer
    await buffer.disconnect()


@pytest.mark.asyncio
async def test_add_and_get_frame(redis_buffer):
    session_uuid = 'test_session'
    frame_data = {
        'frame_track_uuid': '123',
        'timestamp': datetime.now(),
        'exercise': 'squat',
        'session_uuid': session_uuid,
        'landmarks': [{'x': 1, 'y': 2, 'z': 3}],
        'metrics': {'angle': 90}
    }
    await redis_buffer.add_frame(session_uuid, frame_data)

    end_time = datetime.now().timestamp()
    start_time = end_time - 10
    frames = await redis_buffer.get_frames(session_uuid, start_time, end_time)

    assert len(frames) == 1
    assert frames[0]['frame_track_uuid'] == '123'


@pytest.mark.asyncio
async def test_clean_old_frames(redis_buffer):
    session_uuid = 'test_session'
    now = datetime.now()
    old_frame = {
        'frame_track_uuid': 'old',
        'timestamp': now - timedelta(seconds=60),
        'exercise': 'squat',
        'session_uuid': session_uuid,
        'landmarks': [{'x': 0, 'y': 0, 'z': 0}],
        'metrics': {'angle': 45}
    }
    new_frame = {
        'frame_track_uuid': 'new',
        'timestamp': now,
        'exercise': 'squat',
        'session_uuid': session_uuid,
        'landmarks': [{'x': 1, 'y': 1, 'z': 1}],
        'metrics': {'angle': 90}
    }
    await redis_buffer.add_frame(session_uuid, old_frame)
    await redis_buffer.add_frame(session_uuid, new_frame)

    await redis_buffer.clean_old_frames(session_uuid, 30)

    frames = await redis_buffer.get_frames(session_uuid, 0, now.timestamp())
    assert len(frames) == 1
    assert frames[0]['frame_track_uuid'] == 'new'


@pytest.mark.asyncio
async def test_get_latest_frame(redis_buffer):
    session_uuid = 'test_session'
    frame1 = {
        'frame_track_uuid': '1',
        'timestamp': datetime.now() - timedelta(seconds=10),
        'exercise': 'squat',
        'session_uuid': session_uuid,
        'landmarks': [{'x': 2, 'y': 2, 'z': 2}],
        'metrics': {'angle': 60}
    }
    frame2 = {
        'frame_track_uuid': '2',
        'timestamp': datetime.now(),
        'exercise': 'squat',
        'session_uuid': session_uuid,
        'landmarks': [{'x': 3, 'y': 3, 'z': 3}],
        'metrics': {'angle': 90}
    }
    await redis_buffer.add_frame(session_uuid, frame1)
    await redis_buffer.add_frame(session_uuid, frame2)

    latest_frame = await redis_buffer.get_latest_frame(session_uuid)
    assert latest_frame['frame_track_uuid'] == '2'


@pytest.mark.asyncio
async def test_no_frames(redis_buffer):
    session_uuid = 'empty_session'
    frames = await redis_buffer.get_frames(session_uuid, 0, datetime.now().timestamp())
    assert len(frames) == 0

    latest_frame = await redis_buffer.get_latest_frame(session_uuid)
    assert latest_frame is None


@pytest.mark.asyncio
async def test_multiple_sessions(redis_buffer):
    session1 = 'session1'
    session2 = 'session2'
    frame1 = {
        'frame_track_uuid': '1',
        'timestamp': datetime.now(),
        'exercise': 'squat',
        'session_uuid': session1,
        'landmarks': [{'x': 4, 'y': 4, 'z': 4}],
        'metrics': {'angle': 75}
    }
    frame2 = {
        'frame_track_uuid': '2',
        'timestamp': datetime.now(),
        'exercise': 'pushup',
        'session_uuid': session2,
        'landmarks': [{'x': 5, 'y': 5, 'z': 5}],
        'metrics': {'angle': 85}
    }
    await redis_buffer.add_frame(session1, frame1)
    await redis_buffer.add_frame(session2, frame2)

    frames1 = await redis_buffer.get_frames(session1, 0, datetime.now().timestamp())
    frames2 = await redis_buffer.get_frames(session2, 0, datetime.now().timestamp())

    assert len(frames1) == 1
    assert len(frames2) == 1
    assert frames1[0]['exercise'] == 'squat'
    assert frames2[0]['exercise'] == 'pushup'
