import streamlit as st
import cv2
import os
import tempfile
import base64
import json
import pandas as pd
from moviepy.editor import VideoFileClip
from scenedetect import VideoManager, SceneManager
from scenedetect.detectors import ContentDetector
from openai import OpenAI
import numpy as np
import gspread
from google.oauth2.service_account import Credentials
import time  # 遷移演出用に追加

# --- ページ設定 ---
st.set_page_config(
    page_title="ショート動画アナライザー",
    page_icon="🎬",
    layout="wide"
)

# --- 定数・設定 ---
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

# ★コスト管理のための制限設定★
MAX_VIDEO_DURATION = 60  # 最大動画時間（秒）
MAX_ANALYZE_SCENES = 30  # 解析する最大シーン数
DEFAULT_MIN_SCENE_LEN = 30 # 最小シーン長（フレーム数）。30フレーム≒1秒。

# --- 関数定義: Google Sheets連携 ---
def get_gspread_client():
    """Secretsから認証情報を読み込み、gspreadクライアントを返す"""
    try:
        if "gcp_service_account" not in st.secrets:
            # 開発環境などでSecretsがない場合のフォールバック（またはエラー表示）
            # st.error("SecretsにGoogle認証情報が設定されていません。")
            return None
            
        creds_dict = dict(st.secrets["gcp_service_account"])
        
        if "\\n" in creds_dict["private_key"]:
             creds_dict["private_key"] = creds_dict["private_key"].replace("\\n", "\n")
             
        creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
        client = gspread.authorize(creds)
        return client
    except Exception as e:
        st.error(f"データベース接続エラー: {e}")
        return None

def check_login(username, password):
    """ユーザー名とパスワードを照合し、ユーザー情報を返す"""
    client = get_gspread_client()
    if not client:
        # DB接続できない場合は緊急措置としてデモログインを通すか、エラーにする
        # ここではエラーとして返す
        return None

    try:
        sheet_url = st.secrets["SPREADSHEET_URL"]
        sheet = client.open_by_url(sheet_url).sheet1
        records = sheet.get_all_records()
        
        for i, record in enumerate(records):
            if str(record['username']) == username and str(record['password']) == password:
                record['row_index'] = i + 2
                return record
        return None
    except Exception as e:
        st.error(f"ログイン処理エラー: {e}")
        return None

def update_usage(row_index, current_usage):
    """使用回数を+1する"""
    client = get_gspread_client()
    if not client:
        return False
    
    try:
        sheet_url = st.secrets["SPREADSHEET_URL"]
        sheet = client.open_by_url(sheet_url).sheet1
        header = sheet.row_values(1)
        try:
            col_index = header.index("usage") + 1
        except ValueError:
            st.error("DBエラー: usage列が見つかりません")
            return False
            
        sheet.update_cell(row_index, col_index, current_usage + 1)
        return True
    except Exception as e:
        st.error(f"データ更新エラー: {e}")
        return False

# --- 認証機能 (ログイン画面) ---
def login_screen():
    st.title("🎬 ショート動画アナライザー")
    
    # デザイン調整：カラムを使って中央寄せ風に見せる
    col1, col2, col3 = st.columns([1, 2, 1])
    
    with col2:
        with st.form("login_form"):
            st.subheader("会員ログイン")
            st.caption("スクールから発行されたIDとパスワードを入力してください")
            
            username = st.text_input("ユーザーID", placeholder="例: user01")
            password = st.text_input("パスワード", type="password")
            
            # 少しスペースを空ける
            st.write("") 
            submit = st.form_submit_button("ログイン", use_container_width=True)
            
            if submit:
                if not username or not password:
                    st.warning("⚠️ ユーザーIDとパスワードを入力してください。")
                else:
                    with st.spinner("確認中..."):
                        user_info = check_login(username, password)
                    
                    if user_info:
                        st.session_state["user"] = user_info
                        st.success("認証成功！アプリを起動します...")
                        time.sleep(1) # メッセージを読ませるための短いウェイト
                        st.rerun()
                    else:
                        # ここで赤字(error)ではなく黄色(warning)を使用
                        st.warning("⚠️ ログインできませんでした。\n\nIDまたはパスワードが一致しません。入力ミスがないか（大文字・小文字など）をご確認ください。")

if "user" in st.session_state and st.sidebar.button("ログアウト"):
    del st.session_state["user"]
    st.rerun()

if "user" not in st.session_state:
    login_screen()
    st.stop()

# ログイン済みユーザー情報の取得
user = st.session_state["user"]
limit = int(user['limit'])
usage = int(user['usage'])
remaining = limit - usage

# --- アプリ画面 ---
st.sidebar.markdown(f"**ログイン中:** {user['username']}")
st.sidebar.metric("今月の残り回数", f"{remaining} / {limit}")
st.sidebar.progress(usage / limit if limit > 0 else 0)

if remaining <= 0:
    st.error("今月の上限回数に達しました。プランのアップグレードをご検討ください。")
    st.stop()

# ==========================================
# 解析機能 (メイン)
# ==========================================

st.title("🎬 ショート動画アナライザー")
st.markdown("""
Instagram ReelsやTikTok動画をアップロードして、シーンごとの構成要素（視覚情報、テロップ、音声、演出）を自動分析します。
※解析対象は1分以内の動画に限ります。
""")

try:
    api_key = st.secrets["OPENAI_API_KEY"]
except:
    api_key = os.getenv("OPENAI_API_KEY")

if not api_key:
    st.error("システムエラー: API Key設定不備")
    st.stop()

client = OpenAI(api_key=api_key)

def detect_scenes(video_path, threshold=27.0, min_scene_len=15):
    video_manager = VideoManager([video_path])
    scene_manager = SceneManager()
    scene_manager.add_detector(ContentDetector(threshold=threshold, min_scene_len=min_scene_len))
    video_manager.set_downscale_factor()
    video_manager.start()
    scene_manager.detect_scenes(frame_source=video_manager)
    scene_list = scene_manager.get_scene_list(video_manager.get_base_timecode())
    return scene_list

def extract_frame_as_base64(video_path, time_sec):
    cap = cv2.VideoCapture(video_path)
    fps = cap.get(cv2.CAP_PROP_FPS)
    frame_no = int(time_sec * fps)
    cap.set(cv2.CAP_PROP_POS_FRAMES, frame_no)
    ret, frame = cap.read()
    cap.release()
    if not ret: return None, None
    frame_rgb = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
    _, buffer = cv2.imencode('.jpg', cv2.cvtColor(frame_rgb, cv2.COLOR_RGB2BGR))
    base64_image = base64.b64encode(buffer).decode('utf-8')
    return frame_rgb, base64_image

def transcribe_audio(audio_path):
    try:
        with open(audio_path, "rb") as audio_file:
            transcript = client.audio.transcriptions.create(
                model="whisper-1", file=audio_file, response_format="verbose_json"
            )
        return transcript
    except Exception as e:
        return None

def analyze_image_with_gpt4o(base64_image, scene_no):
    """
    シーン番号を受け取り、冒頭(Scene 1, 2)の場合はフック要素を重点的に分析させる
    """
    
    # 冒頭シーンかどうかの判定コンテキスト
    context_instruction = ""
    if scene_no <= 2:
        context_instruction = "この画像は動画の冒頭（Scene 1または2）です。視聴者の指を止めるための『フック要素（バズる要素）』を重点的に評価してください。"
    
    system_prompt = f"""
    あなたはSNSショート動画（TikTok/Reels）のマーケティング専門家です。
    渡された画像（1フレーム）を分析し、以下のJSON形式のみで出力してください。
    
    {context_instruction}

    出力フォーマット:
    {{
      "visual_content": "画面の状況説明（誰が、どこで、何をしているか）",
      "on_screen_text": "画面に表示されている文字全て",
      "vibes": "雰囲気や演出の意図",
      "psychological_effects": "テキストや視覚情報に含まれる『心理効果』を言語化してください（例：バンドワゴン効果、カリギュラ効果、社会的証明、希少性、権威性など）。特になければ『なし』としてください。",
      "hook_factor": "（シーン1, 2の場合のみ記述、それ以外は『-』）冒頭3秒のフックとして、なぜ視聴者が指を止めるのか？バズる要因となる『意外性』『共感』『違和感』『疑問』などを具体的に評価してください。"
    }}
    """
    try:
        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": [
                    {"type": "text", "text": f"これはScene {scene_no}です。詳細に分析してください。"},
                    {"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{base64_image}"}}
                ]}
            ],
            response_format={"type": "json_object"},
            max_tokens=600
        )
        return json.loads(response.choices[0].message.content)
    except:
        return {
            "visual_content": "Error", 
            "on_screen_text": "Error", 
            "vibes": "Error",
            "psychological_effects": "Error",
            "hook_factor": "Error"
        }

def generate_overall_summary(scene_results):
    if not scene_results: return ""
    combined_text = "\n".join([
        f"Scene {item['Scene No']}: {item['Visual Description']} (Psychology: {item['Psychological Effects']}, Hook: {item['Hook Factor']})"
        for item in scene_results
    ])
    system_prompt = "ショート動画のシーン詳細から、動画全体の概要を3-4行で要約してください。また、全体を通して使われている主要な心理テクニックがあれば一言付け加えてください。"
    try:
        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[{"role": "system", "content": system_prompt}, {"role": "user", "content": combined_text}],
            max_tokens=400
        )
        return response.choices[0].message.content
    except: return "要約エラー"

# --- UI実装 ---

# サイドバー設定
st.sidebar.subheader("解析パラメータ")
threshold = st.sidebar.slider("シーン検出感度", 10.0, 50.0, 27.0)
min_scene_len = st.sidebar.slider("最小シーン長", 10, 60, DEFAULT_MIN_SCENE_LEN)

uploaded_file = st.file_uploader("動画ファイルをアップロード (mp4, mov)", type=["mp4", "mov"])

if uploaded_file is not None:
    tfile = tempfile.NamedTemporaryFile(delete=False, suffix='.mp4')
    tfile.write(uploaded_file.read())
    video_path = tfile.name
    tfile.close()

    # 動画情報の取得
    try:
        video_clip = VideoFileClip(video_path)
        video_duration = video_clip.duration
        
        # ★1. 動画の長さチェック
        if video_duration > MAX_VIDEO_DURATION:
            st.error(f"動画が長すぎます（{video_duration:.1f}秒）。{MAX_VIDEO_DURATION}秒以内の動画をアップロードしてください。")
            try:
                video_clip.close()
                os.remove(video_path)
            except: pass
            st.stop()
            
        col_spacer1, col_video, col_spacer2 = st.columns([1, 2, 1])
        with col_video:
            st.video(video_path)

        if st.button("🚀 動画を分析開始"):
            if remaining <= 0:
                st.error("上限回数に達しているため実行できません。")
            else:
                status_text = st.empty()
                progress_bar = st.progress(0)
                
                try:
                    # 1. 音声処理
                    status_text.info("🔊 音声を解析中...")
                    audio_path = os.path.join(tempfile.gettempdir(), "temp_audio.mp3")
                    transcript_text = "音声なし"
                    transcript_segments = []
                    if video_clip.audio:
                        video_clip.audio.write_audiofile(audio_path, verbose=False, logger=None)
                        tr_data = transcribe_audio(audio_path)
                        if tr_data:
                            transcript_text = tr_data.text
                            transcript_segments = getattr(tr_data, 'segments', [])
                        if os.path.exists(audio_path): os.remove(audio_path)
                    progress_bar.progress(20)

                    # 2. シーン検出
                    status_text.info("✂️ シーン検出中...")
                    scenes = detect_scenes(video_path, threshold, min_scene_len)
                    if not scenes:
                        scene_data_list = [{'start': 0.0, 'end': video_duration}]
                    else:
                        scene_data_list = [{'start': s[0].get_seconds(), 'end': s[1].get_seconds()} for s in scenes]
                    
                    # ★2. シーン数制限（コスト保護）
                    if len(scene_data_list) > MAX_ANALYZE_SCENES:
                        st.warning(f"シーン数が多すぎるため（{len(scene_data_list)}個）、先頭の{MAX_ANALYZE_SCENES}シーンのみ解析します。")
                        scene_data_list = scene_data_list[:MAX_ANALYZE_SCENES]

                    progress_bar.progress(40)

                    # 3. GPT解析
                    status_text.info(f"👁️ GPT-4oで{len(scene_data_list)}シーンを解析中...")
                    results = []
                    total_scenes = len(scene_data_list)
                    for i, scene in enumerate(scene_data_list):
                        start_sec = scene['start']
                        end_sec = scene['end']
                        mid_sec = start_sec + (end_sec - start_sec) / 2
                        
                        img_rgb, base64_img = extract_frame_as_base64(video_path, mid_sec)
                        if base64_img:
                            # シーン番号を渡して解析
                            analysis = analyze_image_with_gpt4o(base64_img, i + 1)
                            
                            scene_audio = ""
                            for seg in transcript_segments:
                                seg_start = getattr(seg, 'start', seg.get('start') if isinstance(seg, dict) else 0)
                                seg_text = getattr(seg, 'text', seg.get('text') if isinstance(seg, dict) else "")
                                if start_sec <= seg_start < end_sec:
                                    scene_audio += seg_text + " "
                            
                            results.append({
                                "Scene No": i + 1,
                                "Start Time": f"{start_sec:.2f}s",
                                "End Time": f"{end_sec:.2f}s",
                                "Duration": f"{end_sec-start_sec:.2f}s",
                                "Visual Description": analysis.get("visual_content", ""),
                                "On-Screen Text": analysis.get("on_screen_text", ""),
                                "Vibes": analysis.get("vibes", ""),
                                "Psychological Effects": analysis.get("psychological_effects", ""),
                                "Hook Factor": analysis.get("hook_factor", ""),
                                "Audio Transcript": scene_audio.strip(),
                                "Image Data": img_rgb
                            })
                        progress_bar.progress(40 + int((i + 1) / total_scenes * 60))

                    # 4. 要約生成
                    status_text.info("📝 サマリー生成中...")
                    overall_summary = generate_overall_summary(results)
                    progress_bar.progress(100)
                    status_text.success("✅ 解析完了！")
                    
                    # DB更新
                    new_usage = usage + 1
                    if update_usage(user['row_index'], new_usage):
                        st.session_state["user"]["usage"] = new_usage
                        st.toast(f"残り回数: {limit - new_usage}")
                    else:
                        st.warning("使用回数の更新に失敗しましたが、解析結果は表示します。")

                    # 結果表示
                    st.divider()
                    st.subheader("📊 分析レポート")
                    st.markdown("### 📝 全体概要")
                    c1, c2, c3 = st.columns(3)
                    c1.metric("シーン数", len(results))
                    c2.metric("秒数", f"{video_duration:.1f}s")
                    st.markdown(f"**内容:**\n{overall_summary}")
                    st.divider()
                    st.markdown("### 🎞️ シーン別詳細")

                    export_data = []
                    for item in results:
                        st.markdown(f"#### Scene {item['Scene No']} ({item['Start Time']} - {item['End Time']})")
                        
                        # 冒頭シーンの場合、フック要素を強調表示
                        if item["Hook Factor"] and item["Hook Factor"] != "-" and item["Hook Factor"] != "なし":
                            st.info(f"**🎣 冒頭フック要素 (バズ要因):** {item['Hook Factor']}")

                        c1, c2 = st.columns([1, 2])
                        with c1:
                            if item["Image Data"] is not None:
                                st.image(item["Image Data"], use_container_width=True)
                        with c2:
                            st.markdown(f"**🧠 心理効果:** {item['Psychological Effects']}")
                            st.markdown(f"**🖼️ 視覚情報:** {item['Visual Description']}")
                            st.markdown(f"**📝 テロップ:** {item['On-Screen Text']}")
                            st.markdown(f"**🎙️ 音声:** {item['Audio Transcript']}")
                            st.markdown(f"**✨ Vibes:** {item['Vibes']}")
                        st.divider()
                        export_item = item.copy()
                        if "Image Data" in export_item: del export_item["Image Data"]
                        export_data.append(export_item)

                    df = pd.DataFrame(export_data)
                    csv = df.to_csv(index=False).encode('utf-8-sig')
                    st.download_button("📥 CSVダウンロード", csv, "analysis.csv", "text/csv")
                    st.download_button("📥 Markdownダウンロード", df.to_markdown(index=False), "analysis.md", "text/markdown")

                except Exception as e:
                    st.error(f"エラーが発生しました: {e}")
                finally:
                    if 'video_clip' in locals(): video_clip.close()
                    if os.path.exists(video_path): os.remove(video_path)

    except Exception as e:
        st.error(f"動画ファイルの読み込みエラー: {e}")
