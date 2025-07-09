#!/usr/bin/env python3
"""
파일 경로: scripts/analyze_database_status.py

SQLite 데이터베이스 상태 완전 분석 스크립트
- 테이블별 레코드 수 및 구조 분석
- 데이터 품질 체크
- 날짜 범위 및 누락 데이터 확인
- MySQL 마이그레이션을 위한 권장사항 제시
"""
import sys
import os
import sqlite3
from pathlib import Path
from datetime import datetime, timedelta
import json

# 프로젝트 루트를 Python 경로에 추가
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.core.config import Config
from src.core.database import get_database_manager
from sqlalchemy import text


class DatabaseAnalyzer:
    """데이터베이스 상태 분석기"""

    def __init__(self):
        self.config = Config()
        self.db_manager = get_database_manager()
        self.analysis_result = {}

        # SQLite 직접 연결 (상세 분석용)
        self.db_path = self._get_db_path()

    def _get_db_path(self) -> Path:
        """SQLite DB 파일 경로 확인"""
        db_path = Path("./data/stock_data.db")
        if not db_path.exists():
            # 다른 가능한 경로들 확인
            possible_paths = [
                Path("./data/stock_data_dev.db"),
                Path("./stock_data.db"),
                Path("../data/stock_data.db")
            ]

            for path in possible_paths:
                if path.exists():
                    db_path = path
                    break

        return db_path.absolute()

    def analyze_database(self) -> dict:
        """데이터베이스 완전 분석"""
        print("🔍 SQLite 데이터베이스 상태 분석 시작")
        print("=" * 60)

        try:
            # 1. 기본 정보
            self._analyze_basic_info()

            # 2. 테이블 구조 및 레코드 수
            self._analyze_tables()

            # 3. 주식 기본정보 분석
            self._analyze_stocks_table()

            # 4. 일봉 데이터 분석
            self._analyze_daily_tables()

            # 5. 수집 진행상황 분석
            self._analyze_collection_progress()

            # 6. 데이터 품질 체크
            self._analyze_data_quality()

            # 7. 성능 분석
            self._analyze_performance()

            # 8. MySQL 마이그레이션 권장사항
            self._generate_migration_recommendations()

            # 9. 최종 리포트 출력
            self._print_final_report()

            return self.analysis_result

        except Exception as e:
            print(f"❌ 분석 중 오류 발생: {e}")
            return {"error": str(e)}

    def _analyze_basic_info(self):
        """기본 정보 분석"""
        print("📊 1. 기본 정보 분석")
        print("-" * 30)

        try:
            # DB 파일 정보
            if self.db_path.exists():
                file_size = self.db_path.stat().st_size
                file_size_mb = file_size / (1024 * 1024)

                print(f"📁 DB 파일 경로: {self.db_path}")
                print(f"💾 DB 파일 크기: {file_size_mb:.2f} MB ({file_size:,} bytes)")

                # 마지막 수정 시간
                mtime = datetime.fromtimestamp(self.db_path.stat().st_mtime)
                print(f"📅 마지막 수정: {mtime.strftime('%Y-%m-%d %H:%M:%S')}")

                self.analysis_result['basic_info'] = {
                    'file_path': str(self.db_path),
                    'file_size_mb': round(file_size_mb, 2),
                    'file_size_bytes': file_size,
                    'last_modified': mtime.isoformat()
                }
            else:
                print(f"❌ DB 파일을 찾을 수 없습니다: {self.db_path}")
                self.analysis_result['basic_info'] = {'error': 'DB 파일 없음'}

        except Exception as e:
            print(f"❌ 기본 정보 분석 실패: {e}")

    def _analyze_tables(self):
        """테이블 구조 및 레코드 수 분석"""
        print(f"\n📋 2. 테이블 구조 분석")
        print("-" * 30)

        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()

                # 전체 테이블 목록
                cursor.execute("""
                    SELECT name, type FROM sqlite_master 
                    WHERE type IN ('table', 'view') 
                    ORDER BY name
                """)

                tables = cursor.fetchall()

                print(f"📊 총 테이블/뷰 개수: {len(tables)}개")
                print()

                table_info = {}

                for table_name, table_type in tables:
                    try:
                        # 레코드 수 조회
                        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                        count = cursor.fetchone()[0]

                        # 테이블 스키마 정보
                        cursor.execute(f"PRAGMA table_info({table_name})")
                        columns = cursor.fetchall()

                        table_info[table_name] = {
                            'type': table_type,
                            'record_count': count,
                            'column_count': len(columns),
                            'columns': [(col[1], col[2]) for col in columns]  # (name, type)
                        }

                        print(f"📋 {table_name} ({table_type})")
                        print(f"   📊 레코드 수: {count:,}개")
                        print(f"   🏷️ 컬럼 수: {len(columns)}개")

                        if table_name.startswith('daily_prices_'):
                            stock_code = table_name.replace('daily_prices_', '')
                            print(f"   📈 종목코드: {stock_code}")

                        print()

                    except Exception as e:
                        print(f"❌ {table_name} 분석 실패: {e}")

                self.analysis_result['tables'] = table_info

        except Exception as e:
            print(f"❌ 테이블 분석 실패: {e}")

    def _analyze_stocks_table(self):
        """stocks 테이블 상세 분석"""
        print(f"📈 3. 주식 기본정보 (stocks) 분석")
        print("-" * 30)

        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()

                # 기본 통계
                cursor.execute("SELECT COUNT(*) FROM stocks")
                total_stocks = cursor.fetchone()[0]

                # 시장별 분포
                cursor.execute("""
                    SELECT market, COUNT(*) 
                    FROM stocks 
                    GROUP BY market 
                    ORDER BY COUNT(*) DESC
                """)
                market_dist = cursor.fetchall()

                # 활성/비활성 분포
                cursor.execute("""
                    SELECT is_active, COUNT(*) 
                    FROM stocks 
                    GROUP BY is_active
                """)
                active_dist = cursor.fetchall()

                # 최신 업데이트 현황
                cursor.execute("""
                    SELECT 
                        DATE(last_updated) as update_date, 
                        COUNT(*) as count
                    FROM stocks 
                    WHERE last_updated IS NOT NULL
                    GROUP BY DATE(last_updated)
                    ORDER BY update_date DESC
                    LIMIT 10
                """)
                update_dist = cursor.fetchall()

                # 가격 범위 분석
                cursor.execute("""
                    SELECT 
                        MIN(current_price) as min_price,
                        MAX(current_price) as max_price,
                        AVG(current_price) as avg_price,
                        COUNT(CASE WHEN current_price > 0 THEN 1 END) as valid_prices
                    FROM stocks
                """)
                price_stats = cursor.fetchone()

                print(f"📊 총 종목 수: {total_stocks:,}개")
                print()

                print("📈 시장별 분포:")
                for market, count in market_dist:
                    percentage = (count / total_stocks * 100) if total_stocks > 0 else 0
                    print(f"   {market or 'NULL'}: {count:,}개 ({percentage:.1f}%)")
                print()

                print("🔄 활성 상태 분포:")
                for is_active, count in active_dist:
                    status = "활성" if is_active == 1 else "비활성"
                    percentage = (count / total_stocks * 100) if total_stocks > 0 else 0
                    print(f"   {status}: {count:,}개 ({percentage:.1f}%)")
                print()

                print("📅 최근 업데이트 현황:")
                for update_date, count in update_dist[:5]:
                    print(f"   {update_date}: {count:,}개")
                print()

                if price_stats:
                    min_price, max_price, avg_price, valid_prices = price_stats
                    print("💰 가격 정보:")
                    print(f"   최저가: {min_price:,}원")
                    print(f"   최고가: {max_price:,}원")
                    print(f"   평균가: {avg_price:,.0f}원")
                    print(f"   유효 가격: {valid_prices:,}개")

                self.analysis_result['stocks_analysis'] = {
                    'total_count': total_stocks,
                    'market_distribution': dict(market_dist),
                    'active_distribution': dict(active_dist),
                    'recent_updates': dict(update_dist),
                    'price_stats': {
                        'min': min_price,
                        'max': max_price,
                        'avg': round(avg_price, 2) if avg_price else 0,
                        'valid_count': valid_prices
                    }
                }

        except Exception as e:
            print(f"❌ stocks 테이블 분석 실패: {e}")

    def _analyze_daily_tables(self):
        """일봉 데이터 테이블들 분석"""
        print(f"\n📊 4. 일봉 데이터 분석")
        print("-" * 30)

        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()

                # daily_prices_ 테이블 목록
                cursor.execute("""
                    SELECT name FROM sqlite_master 
                    WHERE type='table' AND name LIKE 'daily_prices_%'
                    ORDER BY name
                """)

                daily_tables = [row[0] for row in cursor.fetchall()]

                print(f"📈 일봉 테이블 개수: {len(daily_tables)}개")

                if not daily_tables:
                    print("⚠️ 일봉 데이터 테이블이 없습니다.")
                    self.analysis_result['daily_analysis'] = {
                        'table_count': 0,
                        'total_records': 0,
                        'average_records_per_stock': 0,
                        'date_range': None,
                        'sample_stocks': {}
                    }
                    return

                # 각 테이블 분석
                total_records = 0
                date_ranges = []
                stock_analysis = {}

                print(f"\n📊 종목별 일봉 데이터 분석 (상위 10개):")

                for i, table in enumerate(daily_tables[:10]):
                    stock_code = table.replace('daily_prices_', '')

                    try:
                        # 레코드 수
                        cursor.execute(f"SELECT COUNT(*) FROM {table}")
                        count = cursor.fetchone()[0]
                        total_records += count

                        # 날짜 범위
                        cursor.execute(f"""
                            SELECT MIN(date), MAX(date) 
                            FROM {table} 
                            WHERE date IS NOT NULL
                        """)
                        date_range = cursor.fetchone()

                        if date_range and date_range[0]:
                            min_date, max_date = date_range
                            date_ranges.append((min_date, max_date))

                            print(f"   📈 {stock_code}: {count:,}개 레코드 ({min_date} ~ {max_date})")

                            stock_analysis[stock_code] = {
                                'record_count': count,
                                'date_range': (min_date, max_date)
                            }
                        else:
                            print(f"   📈 {stock_code}: {count:,}개 레코드 (날짜 정보 없음)")

                    except Exception as e:
                        print(f"❌ {table} 분석 실패: {e}")
                        continue

                # 나머지 테이블들도 카운트에 포함 (상세 정보 없이)
                for table in daily_tables[10:]:
                    try:
                        cursor.execute(f"SELECT COUNT(*) FROM {table}")
                        count = cursor.fetchone()[0]
                        total_records += count
                    except:
                        continue

                if len(daily_tables) > 10:
                    print(f"   ... 외 {len(daily_tables) - 10}개 종목")

                # 전체 통계
                print(f"\n📊 전체 일봉 데이터 통계:")
                print(f"   📈 총 레코드 수: {total_records:,}개")

                if date_ranges:
                    overall_min = min(dr[0] for dr in date_ranges)
                    overall_max = max(dr[1] for dr in date_ranges)
                    print(f"   📅 전체 날짜 범위: {overall_min} ~ {overall_max}")
                else:
                    overall_min = overall_max = None

                # 평균 레코드 수
                if daily_tables:
                    avg_records = total_records / len(daily_tables)
                    print(f"   📊 종목당 평균: {avg_records:.0f}개 레코드")
                else:
                    avg_records = 0

                self.analysis_result['daily_analysis'] = {
                    'table_count': len(daily_tables),
                    'total_records': total_records,
                    'average_records_per_stock': round(avg_records, 0) if daily_tables else 0,
                    'date_range': (overall_min, overall_max) if date_ranges else None,
                    'sample_stocks': stock_analysis
                }

        except Exception as e:
            print(f"❌ 일봉 데이터 분석 실패: {e}")

    def _analyze_collection_progress(self):
        """수집 진행상황 분석"""
        print(f"\n📋 5. 수집 진행상황 분석")
        print("-" * 30)

        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()

                # collection_progress 테이블 존재 확인
                cursor.execute("""
                    SELECT name FROM sqlite_master 
                    WHERE type='table' AND name='collection_progress'
                """)

                if not cursor.fetchone():
                    print("ℹ️ collection_progress 테이블이 없습니다.")
                    return

                # 상태별 분포
                cursor.execute("""
                    SELECT status, COUNT(*) 
                    FROM collection_progress 
                    GROUP BY status 
                    ORDER BY COUNT(*) DESC
                """)
                status_dist = cursor.fetchall()

                # 시도 횟수 분포
                cursor.execute("""
                    SELECT attempt_count, COUNT(*) 
                    FROM collection_progress 
                    GROUP BY attempt_count 
                    ORDER BY attempt_count
                """)
                attempt_dist = cursor.fetchall()

                # 최근 활동
                cursor.execute("""
                    SELECT stock_code, status, last_attempt_time, data_count
                    FROM collection_progress 
                    WHERE last_attempt_time IS NOT NULL
                    ORDER BY last_attempt_time DESC 
                    LIMIT 5
                """)
                recent_activity = cursor.fetchall()

                print("📊 상태별 분포:")
                total_progress = sum(count for _, count in status_dist)
                for status, count in status_dist:
                    percentage = (count / total_progress * 100) if total_progress > 0 else 0
                    print(f"   {status}: {count:,}개 ({percentage:.1f}%)")

                print(f"\n🔄 시도 횟수 분포:")
                for attempt, count in attempt_dist:
                    print(f"   {attempt}회: {count:,}개")

                print(f"\n📅 최근 활동 (상위 5개):")
                for stock_code, status, last_time, data_count in recent_activity:
                    print(f"   {stock_code}: {status} (데이터: {data_count}개, 시간: {last_time})")

                self.analysis_result['collection_progress'] = {
                    'status_distribution': dict(status_dist),
                    'attempt_distribution': dict(attempt_dist),
                    'total_tracked': total_progress,
                    'recent_activity': recent_activity
                }

        except Exception as e:
            print(f"❌ 수집 진행상황 분석 실패: {e}")

    def _analyze_data_quality(self):
        """데이터 품질 체크"""
        print(f"\n🔍 6. 데이터 품질 체크")
        print("-" * 30)

        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()

                quality_issues = []

                # stocks 테이블 품질 체크
                print("📈 stocks 테이블 품질:")

                # NULL 값 체크
                cursor.execute("""
                    SELECT 
                        SUM(CASE WHEN name IS NULL THEN 1 ELSE 0 END) as null_names,
                        SUM(CASE WHEN current_price IS NULL OR current_price = 0 THEN 1 ELSE 0 END) as null_prices,
                        SUM(CASE WHEN market IS NULL THEN 1 ELSE 0 END) as null_markets
                    FROM stocks
                """)

                null_stats = cursor.fetchone()
                if null_stats:
                    null_names, null_prices, null_markets = null_stats
                    print(f"   📝 종목명 NULL: {null_names}개")
                    print(f"   💰 가격 NULL/0: {null_prices}개")
                    print(f"   🏢 시장 NULL: {null_markets}개")

                    if null_names > 0:
                        quality_issues.append(f"종목명 NULL {null_names}개")
                    if null_prices > 0:
                        quality_issues.append(f"가격 NULL/0 {null_prices}개")

                # 일봉 데이터 품질 체크 (샘플)
                cursor.execute("""
                    SELECT name FROM sqlite_master 
                    WHERE type='table' AND name LIKE 'daily_prices_%'
                    LIMIT 3
                """)

                sample_tables = [row[0] for row in cursor.fetchall()]

                if sample_tables:
                    print(f"\n📊 일봉 데이터 품질 (샘플 {len(sample_tables)}개):")

                    for table in sample_tables:
                        stock_code = table.replace('daily_prices_', '')

                        cursor.execute(f"""
                            SELECT 
                                COUNT(*) as total,
                                SUM(CASE WHEN close_price IS NULL OR close_price = 0 THEN 1 ELSE 0 END) as null_prices,
                                SUM(CASE WHEN volume IS NULL THEN 1 ELSE 0 END) as null_volumes
                            FROM {table}
                        """)

                        stats = cursor.fetchone()
                        if stats:
                            total, null_prices, null_volumes = stats
                            print(f"   📈 {stock_code}: 가격NULL {null_prices}/{total}, 거래량NULL {null_volumes}/{total}")

                self.analysis_result['data_quality'] = {
                    'stocks_null_stats': null_stats,
                    'quality_issues': quality_issues,
                    'sample_daily_quality': sample_tables
                }

        except Exception as e:
            print(f"❌ 데이터 품질 체크 실패: {e}")

    def _analyze_performance(self):
        """성능 분석"""
        print(f"\n⚡ 7. 성능 분석")
        print("-" * 30)

        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()

                # 인덱스 정보
                cursor.execute("""
                    SELECT name, tbl_name FROM sqlite_master 
                    WHERE type='index' AND name NOT LIKE 'sqlite_%'
                    ORDER BY tbl_name
                """)

                indexes = cursor.fetchall()

                print(f"📊 사용자 정의 인덱스: {len(indexes)}개")
                for idx_name, table_name in indexes[:10]:
                    print(f"   🔍 {idx_name} (테이블: {table_name})")

                if len(indexes) > 10:
                    print(f"   ... 외 {len(indexes) - 10}개")

                # 테이블별 크기 추정 (페이지 수 기반)
                print(f"\n💾 테이블별 크기 추정:")

                major_tables = ['stocks']
                cursor.execute("""
                    SELECT name FROM sqlite_master 
                    WHERE type='table' AND name LIKE 'daily_prices_%'
                    LIMIT 5
                """)
                major_tables.extend([row[0] for row in cursor.fetchall()])

                for table in major_tables:
                    try:
                        cursor.execute(f"SELECT COUNT(*) FROM {table}")
                        count = cursor.fetchone()[0]

                        # 대략적인 크기 계산 (레코드당 평균 100바이트 가정)
                        estimated_size_mb = (count * 100) / (1024 * 1024)

                        print(f"   📋 {table}: {count:,}개 레코드 (~{estimated_size_mb:.2f}MB)")

                    except Exception as e:
                        print(f"   ❌ {table}: 분석 실패")

                self.analysis_result['performance'] = {
                    'index_count': len(indexes),
                    'indexes': indexes,
                    'major_tables_size': major_tables
                }

        except Exception as e:
            print(f"❌ 성능 분석 실패: {e}")

    def _generate_migration_recommendations(self):
        """MySQL 마이그레이션 권장사항"""
        print(f"\n🚀 8. MySQL 마이그레이션 권장사항")
        print("-" * 30)

        try:
            basic_info = self.analysis_result.get('basic_info', {})
            tables_info = self.analysis_result.get('tables', {})
            daily_analysis = self.analysis_result.get('daily_analysis', {})

            recommendations = []

            # 1. 데이터 볼륨 기반 권장사항
            file_size_mb = basic_info.get('file_size_mb', 0)

            if file_size_mb > 100:
                recommendations.append("🔥 DB 크기가 100MB 이상 - MySQL 마이그레이션 강력 권장")
            elif file_size_mb > 50:
                recommendations.append("⚡ DB 크기가 50MB 이상 - MySQL 마이그레이션 권장")
            else:
                recommendations.append("ℹ️ 현재 크기는 작지만 향후 확장을 위해 MySQL 고려")

            # 2. 테이블 수 기반
            table_count = len(tables_info)
            daily_table_count = daily_analysis.get('table_count', 0)

            if daily_table_count > 100:
                recommendations.append("📊 일봉 테이블이 100개 이상 - 파티셔닝 전략 필요")
            elif daily_table_count > 50:
                recommendations.append("📊 일봉 테이블이 많음 - 테이블 통합 고려")

            # 3. 성능 최적화
            recommendations.append("🔍 인덱스 전략: 날짜, 종목코드 복합 인덱스 필수")
            recommendations.append("📅 파티셔닝: 날짜별 파티셔닝으로 쿼리 성능 향상")

            # 4. 스키마 개선
            recommendations.append("🗄️ 통합 테이블: daily_prices 단일 테이블로 통합 권장")
            recommendations.append("📈 새 데이터: 수급 데이터를 위한 스키마 확장 준비")

            # 5. 마이그레이션 전략
            total_records = daily_analysis.get('total_records', 0)

            if total_records > 1000000:  # 100만 레코드 이상
                recommendations.append("🚀 배치 마이그레이션: 종목별로 나누어 이관 필요")
            else:
                recommendations.append("🚀 일괄 마이그레이션: 전체 데이터 한번에 이관 가능")

            print("💡 권장사항:")
            for i, rec in enumerate(recommendations, 1):
                print(f"   {i}. {rec}")

            # MySQL 스키마 예상 크기
            estimated_mysql_size = file_size_mb * 1.2  # MySQL은 일반적으로 20% 더 큼
            print(f"\n💾 예상 MySQL DB 크기: {estimated_mysql_size:.2f}MB")

            self.analysis_result['migration_recommendations'] = {
                'recommendations': recommendations,
                'estimated_mysql_size_mb': round(estimated_mysql_size, 2),
                'migration_priority': 'HIGH' if file_size_mb > 100 else 'MEDIUM'
            }

        except Exception as e:
            print(f"❌ 마이그레이션 권장사항 생성 실패: {e}")

    def _print_final_report(self):
        """최종 리포트 출력"""
        print(f"\n🎯 9. 최종 분석 리포트")
        print("=" * 60)

        try:
            basic_info = self.analysis_result.get('basic_info', {})
            stocks_analysis = self.analysis_result.get('stocks_analysis', {})
            daily_analysis = self.analysis_result.get('daily_analysis', {})

            print(f"📊 데이터베이스 현황 요약:")
            print(f"   💾 DB 크기: {basic_info.get('file_size_mb', 0):.2f}MB")
            print(f"   📈 종목 수: {stocks_analysis.get('total_count', 0):,}개")
            print(f"   📊 일봉 테이블: {daily_analysis.get('table_count', 0)}개")
            print(f"   📋 총 일봉 레코드: {daily_analysis.get('total_records', 0):,}개")

            # 날짜 범위
            date_range = daily_analysis.get('date_range')
            if date_range:
                print(f"   📅 데이터 기간: {date_range[0]} ~ {date_range[1]}")

            print(f"\n🎯 다음 단계 권장:")
            print(f"   1️⃣ MySQL 마이그레이션 계획 수립")
            print(f"   2️⃣ 통합 스키마 설계 (daily_prices 단일 테이블)")
            print(f"   3️⃣ 데이터 품질 개선")
            print(f"   4️⃣ 성능 최적화 (인덱싱, 파티셔닝)")

            # JSON 리포트 저장
            self._save_json_report()

        except Exception as e:
            print(f"❌ 최종 리포트 생성 실패: {e}")

    def _save_json_report(self):
        """JSON 형태로 분석 결과 저장"""
        try:
            output_file = Path("database_analysis_report.json")

            # 분석 결과에 타임스탬프 추가
            self.analysis_result['analysis_timestamp'] = datetime.now().isoformat()
            self.analysis_result['analysis_version'] = "1.0"

            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(self.analysis_result, f, indent=2, ensure_ascii=False, default=str)

            print(f"\n💾 상세 분석 리포트 저장: {output_file}")

        except Exception as e:
            print(f"❌ JSON 리포트 저장 실패: {e}")


def main():
    """메인 실행 함수"""
    print("🔍 SQLite 데이터베이스 상태 분석 도구")
    print("=" * 60)
    print("📋 분석 항목:")
    print("   1. 기본 정보 (파일 크기, 수정 시간)")
    print("   2. 테이블 구조 및 레코드 수")
    print("   3. 주식 기본정보 분석")
    print("   4. 일봉 데이터 분석")
    print("   5. 수집 진행상황")
    print("   6. 데이터 품질 체크")
    print("   7. 성능 분석")
    print("   8. MySQL 마이그레이션 권장사항")
    print("=" * 60)

    try:
        analyzer = DatabaseAnalyzer()

        # 분석 실행
        result = analyzer.analyze_database()

        if 'error' in result:
            print(f"\n❌ 분석 실패: {result['error']}")
            return False

        print(f"\n✅ 데이터베이스 분석 완료!")
        print(f"📄 상세 리포트: database_analysis_report.json")

        return True

    except KeyboardInterrupt:
        print(f"\n\n👋 사용자가 분석을 중단했습니다.")
        return False
    except Exception as e:
        print(f"\n❌ 예상치 못한 오류: {e}")
        import traceback
        print(f"스택 트레이스:\n{traceback.format_exc()}")
        return False


class MySQLMigrationPlanner:
    """MySQL 마이그레이션 계획 수립 도구"""

    def __init__(self, analysis_result: dict):
        self.analysis_result = analysis_result

    def generate_migration_plan(self) -> dict:
        """마이그레이션 계획 생성"""
        print(f"\n🚀 MySQL 마이그레이션 계획 수립")
        print("-" * 40)

        plan = {
            'migration_strategy': self._determine_strategy(),
            'schema_design': self._design_mysql_schema(),
            'performance_optimization': self._plan_performance_optimization(),
            'data_migration_steps': self._plan_migration_steps(),
            'estimated_timeline': self._estimate_timeline()
        }

        return plan

    def _determine_strategy(self) -> dict:
        """마이그레이션 전략 결정"""
        daily_analysis = self.analysis_result.get('daily_analysis', {})
        table_count = daily_analysis.get('table_count', 0)
        total_records = daily_analysis.get('total_records', 0)

        if table_count > 100 or total_records > 1000000:
            strategy = "GRADUAL"  # 점진적 마이그레이션
            batch_size = 50  # 한 번에 50개 종목씩
        else:
            strategy = "BULK"  # 일괄 마이그레이션
            batch_size = table_count

        return {
            'type': strategy,
            'batch_size': batch_size,
            'parallel_processing': table_count > 50,
            'downtime_required': strategy == "BULK"
        }

    def _design_mysql_schema(self) -> dict:
        """MySQL 스키마 설계"""
        return {
            'unified_daily_table': {
                'table_name': 'daily_prices',
                'partitioning': 'BY_DATE',  # 날짜별 파티셔닝
                'indexes': [
                    'PRIMARY KEY (stock_code, date)',
                    'INDEX idx_date (date)',
                    'INDEX idx_stock_code (stock_code)',
                    'INDEX idx_volume (volume)'
                ]
            },
            'stocks_table': {
                'enhancements': [
                    'Full-text search on name',
                    'JSON column for extended attributes',
                    'Improved indexing on market, market_cap'
                ]
            },
            'new_tables': [
                'supply_demand_data',  # 수급 데이터
                'minute_data',  # 분봉 데이터
                'market_events'  # 시장 이벤트
            ]
        }

    def _plan_performance_optimization(self) -> dict:
        """성능 최적화 계획"""
        return {
            'indexing_strategy': [
                'Composite indexes for common queries',
                'Covering indexes for read-heavy operations',
                'Partial indexes for filtered queries'
            ],
            'partitioning': {
                'daily_prices': 'RANGE partitioning by date (monthly)',
                'supply_demand': 'RANGE partitioning by date (monthly)',
                'minute_data': 'RANGE partitioning by date (weekly)'
            },
            'caching': [
                'Redis for frequently accessed stock info',
                'Query result caching for dashboard',
                'Connection pooling optimization'
            ]
        }

    def _plan_migration_steps(self) -> list:
        """마이그레이션 단계 계획"""
        return [
            {
                'step': 1,
                'name': 'MySQL 환경 준비',
                'tasks': [
                    'MySQL 서버 설치 및 설정',
                    '데이터베이스 및 사용자 생성',
                    '스키마 생성 스크립트 실행'
                ],
                'estimated_time': '2시간'
            },
            {
                'step': 2,
                'name': 'stocks 테이블 마이그레이션',
                'tasks': [
                    'SQLite에서 stocks 데이터 추출',
                    'MySQL로 데이터 이관',
                    '데이터 무결성 검증'
                ],
                'estimated_time': '30분'
            },
            {
                'step': 3,
                'name': 'daily_prices 통합 마이그레이션',
                'tasks': [
                    '종목별 테이블을 단일 테이블로 통합',
                    '배치별 데이터 이관',
                    '파티셔닝 적용'
                ],
                'estimated_time': '2-4시간'
            },
            {
                'step': 4,
                'name': '인덱스 및 최적화',
                'tasks': [
                    '모든 인덱스 생성',
                    '성능 테스트',
                    '쿼리 최적화'
                ],
                'estimated_time': '1시간'
            },
            {
                'step': 5,
                'name': '애플리케이션 연동',
                'tasks': [
                    '데이터베이스 설정 변경',
                    '연결 테스트',
                    '기능 검증'
                ],
                'estimated_time': '1시간'
            }
        ]

    def _estimate_timeline(self) -> dict:
        """작업 시간 예상"""
        daily_analysis = self.analysis_result.get('daily_analysis', {})
        total_records = daily_analysis.get('total_records', 0)

        # 레코드 수에 따른 시간 계산
        if total_records > 5000000:  # 500만 레코드 이상
            data_migration_hours = 8
        elif total_records > 1000000:  # 100만 레코드 이상
            data_migration_hours = 4
        else:
            data_migration_hours = 2

        return {
            'total_estimated_time': f"{2 + data_migration_hours + 3}시간",
            'preparation': '2시간',
            'data_migration': f'{data_migration_hours}시간',
            'optimization': '2시간',
            'testing': '1시간',
            'recommended_schedule': 'Weekend deployment'
        }


def generate_migration_plan(analysis_file: str = "database_analysis_report.json"):
    """분석 결과를 기반으로 마이그레이션 계획 생성"""
    try:
        # 분석 결과 로드
        with open(analysis_file, 'r', encoding='utf-8') as f:
            analysis_result = json.load(f)

        # 마이그레이션 계획 생성
        planner = MySQLMigrationPlanner(analysis_result)
        plan = planner.generate_migration_plan()

        # 계획 저장
        plan_file = "mysql_migration_plan.json"
        with open(plan_file, 'w', encoding='utf-8') as f:
            json.dump(plan, f, indent=2, ensure_ascii=False, default=str)

        print(f"📋 마이그레이션 계획 저장: {plan_file}")

        return plan

    except Exception as e:
        print(f"❌ 마이그레이션 계획 생성 실패: {e}")
        return None


if __name__ == "__main__":
    try:
        # 1. 데이터베이스 분석 실행
        print("🚀 1단계: 데이터베이스 상태 분석")
        success = main()

        if success:
            print(f"\n🚀 2단계: MySQL 마이그레이션 계획 수립")
            plan = generate_migration_plan()

            if plan:
                print(f"\n✅ 분석 및 마이그레이션 계획 수립 완료!")
                print(f"📄 생성된 파일:")
                print(f"   📊 database_analysis_report.json")
                print(f"   🚀 mysql_migration_plan.json")
                print(f"\n💡 다음 단계:")
                print(f"   1. 분석 리포트 검토")
                print(f"   2. 마이그레이션 계획 승인")
                print(f"   3. MySQL 환경 준비")
                print(f"   4. 마이그레이션 실행")
            else:
                print(f"⚠️ 마이그레이션 계획 수립은 실패했지만 분석은 완료됨")
        else:
            print(f"❌ 데이터베이스 분석 실패")

    except Exception as e:
        print(f"❌ 실행 중 오류: {e}")
        sys.exit(1)