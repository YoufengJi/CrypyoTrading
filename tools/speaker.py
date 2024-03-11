from gtts import gTTS
from playsound import playsound
import os


def speak(text, lang='zh-cn'):
    """在播放语音警告之前播放铃声

    参数:
    text (str): 要转换成语音的文本
    lang (str, optional): 语音的语言。默认为'zh-cn'（中文）。
    """
    # 首先播放铃声
    bell_sound_file = "config/notify_bell.wav"  # 替换为铃声文件的正确路径
    playsound(bell_sound_file)

    # 生成语音
    tts = gTTS(text=text, lang=lang, slow=False)
    # 保存到临时文件
    temp_file = "temp.mp3"
    tts.save(temp_file)
    # 播放语音
    playsound(temp_file)
    # 删除临时文件
    os.remove(temp_file)


