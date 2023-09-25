# Workload_Prediction

IITP-2021-0-00256 클라우드 자원의 지능적 관리를 위한 이종 가상화(VM+Container) 통합 운용 기술 개발 과제를 통해 공개한 오케스트로의 워크로드 예측 알고리즘

### Directory Explanation

* Dataset : 워크로드 예측을 위한 알리바바사의 벤치마크 데이터셋 폴더

  > alibabaCloudTrace.py : 모델 학습을 위한 벤치마크 데이터셋 로드 코드

* Workload_Prediction : 워크로드 예측을 위한 관련 코드 폴더

  > SYMPHONY_WORKLOAD_DECOMPOSITION_MAIN.py : Decomposition을 위해 사용되는 메인 함수 코드
  
  > SYMPHONY_WORKLOAD_DECOMPOSITION_PIPELINE.py :  Decomposition을 위해 사용되는 파이프라인 모듈 설계 코드
  
  > SYMPHONY_WORKLOAD_PREDICTION_PIPELINE.py : 워크로드 예측을 위한 파이프라인 모듈 설계 코드
    
  > SYMPHONY_WORKLOAD_TEST_PREDICTION.py : 학습된 모델 바탕의 워크로드 예측 추론 코드
    

### Who We Are
회사 홈페이지:
http://okestro.com/

### License
MIT License
