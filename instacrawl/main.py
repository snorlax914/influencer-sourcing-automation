"""단일 계정 정보 확인용 데모 스크립트.

계정명을 이미 알고 있을 때 그 프로필/게시물 정보를 출력한다.
새 인플루언서 '발굴'은 discover.py를 사용한다.
"""
import instaloader

from session import (
    L,
    enable_request_logging,
    login_with_browser_cookies,
    safe,
)

# 가져올 인플루언서 계정명과 최대 게시물 수
TARGET_USERNAME = "cristiano"
MAX_POSTS = 10
MAX_REELS = 5  # 릴스는 개당 추가 요청이 들어가므로 적게


def print_reels(profile) -> None:
    """프로필의 릴스 목록을 최대 MAX_REELS개 출력한다.

    주의: get_reels()는 graphql/query(403 잘 나는 엔드포인트)를 쓰고,
    릴스 하나마다 추가 요청을 보낸다. 막히면 예외를 출력하고 넘어간다.
    """
    print("=" * 30)
    print(f"[릴스 목록 최대 {MAX_REELS}개]")
    try:
        for i, reel in enumerate(profile.get_reels()):
            if i >= MAX_REELS:
                break
            print(f"릴스 URL: https://www.instagram.com/reel/{safe(lambda: reel.shortcode, '?')}/")
            print(f"  게시일: {safe(lambda: reel.date, '(불러올 수 없음)')}")
            print(f"  좋아요: {safe(lambda: reel.likes, '(불러올 수 없음)')}")
            print(f"  조회수: {safe(lambda: reel.video_view_count, '(불러올 수 없음)')}")
            print(f"  본문: {safe(lambda: reel.caption, '(불러올 수 없음)')}")
            print("-" * 30)
    except Exception as e:
        print(f"릴스 조회 실패 [{type(e).__name__}]: {e}")
        print("→ 인스타가 릴스(GraphQL) 조회를 막았습니다. 계정 상태/차단 문제일 수 있습니다.")


def main() -> None:
    enable_request_logging()
    username = login_with_browser_cookies()
    if not username:
        print("로그인에 실패해 조회를 중단합니다.")
        return

    # 타겟 인플루언서 프로필 조회
    try:
        profile = instaloader.Profile.from_username(L.context, TARGET_USERNAME)
    except instaloader.exceptions.ProfileNotExistsException:
        print(f"'{TARGET_USERNAME}' 계정이 존재하지 않습니다.")
        return
    except Exception as e:
        # 실제 원인을 감추지 말고 예외 종류와 메시지를 그대로 출력한다.
        print(f"프로필 조회 중 오류 [{type(e).__name__}]: {e}")
        return

    # 기본 정보 출력
    print(f"이름: {profile.full_name}")
    print(f"팔로워 수: {profile.followers}")
    print(f"팔로잉 수: {profile.followees}")
    print(f"소개글: {profile.biography}")
    print("=" * 30)

    # 최근 게시물 MAX_POSTS개 가져오기
    try:
        for i, post in enumerate(profile.get_posts()):
            # 제한을 피하기 위해 MAX_POSTS개만 받고 중단
            if i >= MAX_POSTS:
                break
            # 일부 필드(예: 댓글 수)는 추가 상세조회가 필요한데, 인스타가
            # 해당 GraphQL 요청을 403으로 막을 수 있다. 필드별로 안전하게
            # 접근해 한 필드가 실패해도 나머지는 계속 출력한다.
            print(f"게시일: {safe(lambda: post.date, '(불러올 수 없음)')}")
            print(f"좋아요 수: {safe(lambda: post.likes, '(불러올 수 없음)')}")
            print(f"댓글 수: {safe(lambda: post.comments, '(불러올 수 없음)')}")
            print(f"본문: {safe(lambda: post.caption, '(불러올 수 없음)')}")
            print("-" * 30)
    except instaloader.exceptions.ConnectionException as e:
        print(f"게시물 조회 중 오류가 발생했습니다: {e}")

    # 릴스 목록 조회
    print_reels(profile)


if __name__ == "__main__":
    main()
