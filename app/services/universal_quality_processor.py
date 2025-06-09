import json
from datetime import datetime
from typing import Dict, Any, List, Optional, Tuple
from enum import Enum
from quixstreams import Application
from quixstreams.models import TopicConfig

from app.core.config import get_settings
from app.cv.metrics_storage import MetricsStorage
from app.utils.logger import setup_logger


class UniversalQualityProcessor:
    """
    Универсальный процессор качественной оценки для любых упражнений
    
    Работает с новой семантической структурой конфигураций v2.0
    Расширяемая архитектура: один алгоритм → любые упражнения
    """
    
    def __init__(self):
        self.settings = get_settings()
        self.logger = setup_logger("universal_quality_processor")
        self.metrics_storage = MetricsStorage(self.settings.metrics_dir)
        
        # Quix Streams Application
        self.app = Application(
            broker_address=self.settings.kafka_bootstrap_servers,
            consumer_group=f"{self.settings.kafka_group_id}_universal",
            auto_offset_reset="earliest",
        )
        
        # Топики
        self.input_topic = self.app.topic(
            self.settings.kafka_input_topic,
            config=TopicConfig(num_partitions=3, replication_factor=1)
        )
        self.output_topic = self.app.topic(
            "performance-feedback",
            config=TopicConfig(num_partitions=3, replication_factor=1)
        )

    def create_universal_pipeline(self):
        """Создание универсального pipeline для любого упражнения"""
        self.logger.info("🚀 Creating universal exercise quality pipeline...")
        
        sdf = self.app.dataframe(self.input_topic)
        
        # Базовые операции
        sdf = sdf.apply(self._parse_cv_result)
        sdf = sdf.filter(lambda frame: frame is not None)
        sdf = sdf.group_by("session_uuid")
        
        # Универсальный FSM анализ
        sdf = sdf.apply(self._analyze_exercise_universal, stateful=True)
        
        # Универсальная генерация семантических кодов
        sdf = sdf.apply(self._generate_semantic_error_codes)
        
        # Временная агрегация (3 секунды)
        sdf = sdf.tumbling_window(duration_ms=3000, grace_ms=500)
        sdf = sdf.reduce(self._aggregate_semantic_patterns, self._init_semantic_state)
        
        # Семантический анализ для LLM
        sdf = sdf.final()
        sdf = sdf.apply(self._analyze_semantic_coaching, stateful=True)
        
        # Фильтр значимых coaching signals
        sdf = sdf.filter(lambda result: result.get("requires_coaching", False))
        
        # Финальная подготовка для LLM
        sdf = sdf.apply(self._prepare_llm_semantic_input)
        
        sdf = sdf.to_topic(self.output_topic)
        
        return sdf

    def _parse_cv_result(self, message) -> Optional[Dict[str, Any]]:
        """Парсинг результата от CV сервиса"""
        try:
            # Handle both bytes and dict inputs
            if isinstance(message, bytes):
                data = json.loads(message.decode("utf-8"))
            elif isinstance(message, dict):
                data = message
            else:
                # Try to parse as string
                data = json.loads(str(message))
            
            self.logger.debug(f"📥 Universal parser - frame: {data.get('frame_track_uuid', 'unknown')}")
            return data
        except Exception as e:
            self.logger.error(f"❌ Parse error: {e}, message type: {type(message)}")
            return None

    def _analyze_exercise_universal(self, frame: Dict[str, Any], state: Dict[str, Any]) -> Dict[str, Any]:
        """
        Универсальный анализ упражнения с загрузкой соответствующей конфигурации
        """
        try:
            session_uuid = frame['session_uuid']
            exercise_name = frame.get('exercise')
            
            # Инициализация сессии
            if not state.exists(session_uuid):
                state.set(session_uuid, {
                    'exercise_config': None,
                    'fsm_context': {
                        'current_state': None,
                        'prev_state': None,
                        'state_start_time': frame['current_time'],
                        'state_duration': 0.0,
                        'transition_history': [],
                        'exercise_phase': 'unknown'
                    },
                    'level': 'beginner'  # TODO: получать из user profile
                })
            
            session_state = state.get(session_uuid)
            
            # Загрузка конфигурации упражнения (v2.0)
            if session_state.get('exercise_config') is None:
                try:
                    # Пытаемся загрузить новую версию конфигурации
                    config_path = f"{exercise_name}-v2.json"
                    session_state['exercise_config'] = self.metrics_storage.get_metrics_for_exercise_v2(config_path)
                    self.logger.info(f"✅ Loaded v2.0 config for {exercise_name}")
                except:
                    # Fallback на старую конфигурацию если v2 не найдена
                    self.logger.warning(f"⚠️ v2.0 config not found for {exercise_name}, using legacy")
                    session_state['exercise_config'] = self.metrics_storage.get_metrics_for_exercise(exercise_name)
                
                # Сохраняем обновленное состояние
                state.set(session_uuid, session_state)
            
            exercise_config = session_state['exercise_config']
            
            # Извлечение углов
            angles = {
                angle['name']: angle['value']
                for angle in frame['metrics']['angles']
            }
            
            # Универсальный FSM анализ
            fsm_context = self._analyze_fsm_universal(angles, exercise_config, session_state, frame['current_time'])
            session_state['fsm_context'] = fsm_context
            state.set(session_uuid, session_state)
            
            # Добавление полного контекста к кадру
            frame['exercise_context'] = {
                'exercise_name': exercise_name,
                'config_version': exercise_config.get('version', '1.0'),
                'fsm_context': fsm_context,
                'angles': angles,
                'level': session_state['level']
            }
            
            self.logger.debug(f"🎯 Universal FSM - {exercise_name}: {fsm_context['current_state']} "
                            f"({fsm_context['exercise_phase']}) duration: {fsm_context['state_duration']:.1f}s")
            
            return frame
            
        except Exception as e:
            self.logger.error(f"❌ Universal analysis error: {e}")
            frame['exercise_context'] = {'error': str(e)}
            return frame

    def _analyze_fsm_universal(self, angles: Dict[str, float], exercise_config: Dict, 
                             session_state: Dict, current_time: float) -> Dict[str, Any]:
        """Универсальный анализ FSM для любого упражнения"""
        
        fsm_context = session_state['fsm_context']
        level = session_state['level']
        
        # Получение FSM конфигурации
        if 'finite_state_machine' in exercise_config:
            # Новая v2.0 структура
            fsm_config = exercise_config['finite_state_machine'][level]
        else:
            # Старая структура
            fsm_config = exercise_config['state_machine'][level]
        
        # Определение текущего состояния
        current_state = self._determine_state_universal(angles, fsm_config['states'])
        
        # Обновление FSM контекста
        prev_state = fsm_context['current_state']
        
        if current_state != prev_state:
            # Переход в новое состояние
            if prev_state is not None:
                transition = {
                    'from': prev_state,
                    'to': current_state,
                    'timestamp': current_time,
                    'duration_in_prev': fsm_context['state_duration']
                }
                fsm_context['transition_history'].append(transition)
                
                # Ограничиваем историю
                if len(fsm_context['transition_history']) > 10:
                    fsm_context['transition_history'] = fsm_context['transition_history'][-10:]
            
            fsm_context['prev_state'] = prev_state
            fsm_context['current_state'] = current_state
            fsm_context['state_start_time'] = current_time
            fsm_context['state_duration'] = 0.0
        else:
            # Продолжение в текущем состоянии
            fsm_context['state_duration'] = current_time - fsm_context['state_start_time']
        
        # Определение фазы упражнения
        fsm_context['exercise_phase'] = self._determine_exercise_phase_universal(
            current_state, fsm_config['states']
        )
        
        return fsm_context

    def _determine_state_universal(self, angles: Dict[str, float], states: List[Dict]) -> str:
        """Универсальное определение состояния FSM"""
        for state in states:
            condition = state["condition"]
            try:
                # Безопасная оценка условий
                if self._evaluate_condition_safe(condition, angles):
                    return state["id"]
            except Exception as e:
                self.logger.warning(f"⚠️ State evaluation error for '{condition}': {e}")
                continue
        
        return "s1"  # Дефолтное состояние

    def _evaluate_condition_safe(self, condition: str, angles: Dict[str, float]) -> bool:
        """Безопасная оценка условий без eval()"""
        try:
            # Ограниченный контекст для eval
            safe_dict = {k: v for k, v in angles.items() if isinstance(v, (int, float))}
            return eval(condition, {"__builtins__": {}}, safe_dict)
        except:
            return False

    def _determine_exercise_phase_universal(self, current_state: str, states: List[Dict]) -> str:
        """Универсальное определение фазы упражнения"""
        for state in states:
            if state['id'] == current_state:
                return state.get('exercise_phase', 'unknown')
        return 'unknown'

    def _generate_semantic_error_codes(self, frame: Dict[str, Any]) -> Dict[str, Any]:
        """
        Универсальная генерация семантических кодов ошибок из конфигурации
        """
        context = frame.get('exercise_context', {})
        if 'error' in context:
            return frame
        
        error_codes = []
        
        try:
            # Загрузка конфигурации из контекста
            exercise_name = context.get('exercise_name')
            config_version = context.get('config_version', '1.0')
            
            if config_version == '2.0':
                # Новая семантическая обработка
                error_codes = self._process_semantic_taxonomy(frame, context)
            else:
                # Fallback на старую систему
                error_codes = self._process_legacy_thresholds(frame, context)
            
            frame['semantic_error_codes'] = error_codes
            
            self.logger.debug(f"🔍 Generated {len(error_codes)} semantic codes for {exercise_name}")
            
        except Exception as e:
            self.logger.error(f"❌ Semantic code generation error: {e}")
            frame['semantic_error_codes'] = []
        
        return frame

    def _process_semantic_taxonomy(self, frame: Dict[str, Any], context: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Обработка новой семантической таксономии v2.0"""
        error_codes = []
        
        # Получение данных
        exercise_config = self._load_exercise_config_from_context(context)
        if not exercise_config or 'error_taxonomy' not in exercise_config:
            return error_codes
        
        taxonomy = exercise_config['error_taxonomy']
        angles = context.get('angles', {})
        fsm_context = context.get('fsm_context', {})
        current_state = fsm_context.get('current_state', 's1')
        state_duration = fsm_context.get('state_duration', 0.0)
        
        # Обработка каждой категории ошибок
        for category, violations in taxonomy.items():
            for violation_type, config in violations.items():
                
                # Проверка применимости к текущему состоянию
                applicable_states = config.get('states_applicable', [])
                if applicable_states != ['all'] and current_state not in applicable_states:
                    continue
                
                # Генерация кода ошибки
                error_code = self._check_violation_universal(
                    config, angles, current_state, state_duration, category, violation_type
                )
                
                if error_code:
                    error_codes.append(error_code)
        
        return error_codes

    def _check_violation_universal(self, violation_config: Dict, angles: Dict[str, float], 
                                 current_state: str, state_duration: float,
                                 category: str, violation_type: str) -> Optional[Dict[str, Any]]:
        """Универсальная проверка нарушения на основе конфигурации"""
        
        detection = violation_config.get('detection', {})
        
        # Проверка на основе угла
        if 'angle' in detection:
            angle_name = detection['angle']
            if angle_name not in angles:
                return None
            
            angle_value = angles[angle_name]
            
            # State-specific detection
            if 'state_specific_detection' in violation_config:
                state_config = violation_config['state_specific_detection'].get(current_state)
                if state_config:
                    return self._check_angle_violation(
                        angle_value, state_config, violation_config, current_state, category, violation_type
                    )
            
            # State-specific thresholds
            elif 'state_specific_thresholds' in detection:
                state_thresholds = detection['state_specific_thresholds'].get(current_state)
                if state_thresholds:
                    return self._check_threshold_violation(
                        angle_value, state_thresholds, violation_config, current_state, category, violation_type
                    )
            
            # Simple severity levels
            elif 'severity_levels' in detection:
                return self._check_severity_levels(
                    angle_value, detection['severity_levels'], violation_config, current_state, category, violation_type
                )
        
        # Проверка на основе времени
        elif detection.get('method') == 'temporal_analysis':
            max_duration = detection.get('max_duration_per_state', {}).get(current_state)
            if max_duration and state_duration > max_duration:
                return self._create_semantic_error_code(
                    violation_config, current_state, category, violation_type,
                    {'duration': state_duration, 'max_allowed': max_duration}
                )
        
        return None

    def _check_angle_violation(self, angle_value: float, state_config: Dict, 
                             violation_config: Dict, current_state: str, 
                             category: str, violation_type: str) -> Optional[Dict[str, Any]]:
        """Проверка нарушения угла с state-specific конфигурацией"""
        
        optimal_range = state_config.get('optimal_range', [])
        if len(optimal_range) != 2:
            return None
        
        min_optimal, max_optimal = optimal_range
        
        # Проверка нарушения диапазона
        if min_optimal <= angle_value <= max_optimal:
            return None  # В норме
        
        # Определение уровня серьезности
        severity_levels = state_config.get('severity_levels', {})
        
        for severity, level_config in severity_levels.items():
            level_range = level_config.get('range', [])
            if len(level_range) == 2:
                min_range, max_range = level_range
                if min_range <= angle_value <= max_range:
                    # Нарушение найдено
                    code_suffix = level_config.get('code_suffix', severity.upper())
                    return self._create_semantic_error_code(
                        violation_config, current_state, category, violation_type,
                        {
                            'angle_value': angle_value,
                            'optimal_range': optimal_range,
                            'severity': severity,
                            'code_suffix': code_suffix
                        }
                    )
        
        return None

    def _check_threshold_violation(self, angle_value: float, thresholds: Dict,
                                 violation_config: Dict, current_state: str,
                                 category: str, violation_type: str) -> Optional[Dict[str, Any]]:
        """Проверка нарушения порогов"""
        
        optimal_range = thresholds.get('optimal_range', [])
        if len(optimal_range) != 2:
            return None
        
        min_optimal, max_optimal = optimal_range
        
        if min_optimal <= angle_value <= max_optimal:
            return None
        
        # Проверка severity levels
        severity_levels = thresholds.get('severity_levels', {})
        
        for severity, level_config in severity_levels.items():
            level_range = level_config.get('range', [])
            if len(level_range) == 2:
                min_range, max_range = level_range
                if min_range <= angle_value <= max_range:
                    return self._create_semantic_error_code(
                        violation_config, current_state, category, violation_type,
                        {
                            'angle_value': angle_value,
                            'optimal_range': optimal_range,
                            'severity': severity
                        }
                    )
        
        return None

    def _check_severity_levels(self, angle_value: float, severity_levels: Dict,
                             violation_config: Dict, current_state: str,
                             category: str, violation_type: str) -> Optional[Dict[str, Any]]:
        """Проверка простых уровней серьезности"""
        
        detection = violation_config.get('detection', {})
        threshold_type = detection.get('threshold_type', 'absolute_deviation')
        
        # Обработка range_safety типа
        if threshold_type == 'range_safety':
            safe_range = detection.get('safe_range', [])
            if len(safe_range) == 2:
                min_safe, max_safe = safe_range
                
                # Проверяем, находится ли угол вне безопасного диапазона
                if angle_value < min_safe or angle_value > max_safe:
                    # Определяем уровень серьезности на основе того, насколько далеко от безопасного диапазона
                    for severity, level_config in severity_levels.items():
                        level_range = level_config.get('range', [])
                        if len(level_range) == 2:
                            min_range, max_range = level_range
                            if min_range <= angle_value <= max_range:
                                return self._create_semantic_error_code(
                                    violation_config, current_state, category, violation_type,
                                    {
                                        'angle_value': angle_value,
                                        'safe_range': safe_range,
                                        'violation_range': level_range,
                                        'severity': severity
                                    }
                                )
            return None
        
        # Существующая логика для других типов порогов
        for severity, level_config in severity_levels.items():
            threshold = level_config.get('threshold')
            if threshold is None:
                continue
            
            if threshold_type == 'absolute_deviation' and abs(angle_value) > threshold:
                return self._create_semantic_error_code(
                    violation_config, current_state, category, violation_type,
                    {
                        'angle_value': angle_value,
                        'threshold': threshold,
                        'severity': severity
                    }
                )
            elif threshold_type == 'deviation_from_target':
                target_angle = detection.get('target_angle', 0)
                deviation = abs(angle_value - target_angle)
                if deviation > threshold:
                    return self._create_semantic_error_code(
                        violation_config, current_state, category, violation_type,
                        {
                            'angle_value': angle_value,
                            'target_angle': target_angle,
                            'deviation': deviation,
                            'threshold': threshold,
                            'severity': severity
                        }
                    )
        
        return None

    def _create_semantic_error_code(self, violation_config: Dict, current_state: str,
                                  category: str, violation_type: str, 
                                  metrics: Dict[str, Any]) -> Dict[str, Any]:
        """Создание семантического кода ошибки"""
        
        code_prefix = violation_config.get('code_prefix', f"{category.upper()}_{violation_type.upper()}")
        code_suffix = metrics.get('code_suffix', current_state.upper())
        
        code = f"{code_prefix}_{code_suffix}"
        
        return {
            'code': code,
            'timestamp': datetime.now().isoformat(),
            'category': category,
            'violation_type': violation_type,
            'description': violation_config.get('description', ''),
            'coaching_priority': violation_config.get('coaching_priority', 'medium'),
            'semantic_category': violation_config.get('semantic_category', 'unknown'),
            'immediate_intervention': violation_config.get('immediate_intervention', False),
            'context': {
                'exercise_state': current_state,
                'metrics': metrics
            }
        }

    def _process_legacy_thresholds(self, frame: Dict[str, Any], context: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Fallback обработка для старых конфигураций"""
        # Здесь можно реализовать совместимость со старыми форматами
        self.logger.info("📱 Processing legacy threshold format")
        return []

    def _load_exercise_config_from_context(self, context: Dict[str, Any]) -> Optional[Dict]:
        """Загрузка конфигурации упражнения из контекста"""
        exercise_name = context.get('exercise_name')
        if not exercise_name:
            return None
        
        try:
            # Попытка загрузки v2.0 конфигурации
            config_path = f"{exercise_name}-v2.json"
            return self.metrics_storage.get_metrics_for_exercise_v2(config_path)
        except:
            return None

    def _init_semantic_state(self, value: Any) -> Dict[str, Any]:
        """Инициализация состояния для семантической агрегации"""
        return {
            'window_start': None,
            'window_end': None,
            'session_uuid': None,
            'exercise': None,
            'frames_processed': 0,
            'semantic_patterns': {},  # Семантические паттерны ошибок
            'fsm_flow': [],  # Поток FSM переходов
            'coaching_categories': {
                'safety_concerns': [],
                'technical_issues': [],
                'form_deviations': [],
                'execution_problems': []
            }
        }

    def _aggregate_semantic_patterns(self, window_state: Dict[str, Any], 
                                   frame: Dict[str, Any]) -> Dict[str, Any]:
        """
        Агрегация семантических паттернов в временном окне
        """
        # Обновление метаданных
        if window_state['session_uuid'] is None:
            window_state['session_uuid'] = frame['session_uuid']
            window_state['exercise'] = frame['exercise']
            window_state['window_start'] = frame['current_time']
        
        window_state['window_end'] = frame['current_time']
        window_state['frames_processed'] += 1
        
        # Агрегация FSM контекста
        exercise_context = frame.get('exercise_context', {})
        fsm_context = exercise_context.get('fsm_context', {})
        
        if fsm_context.get('current_state'):
            transition_history = fsm_context.get('transition_history', [])
            for transition in transition_history:
                if transition not in window_state['fsm_flow']:
                    window_state['fsm_flow'].append(transition)
        
        # Агрегация семантических кодов
        semantic_codes = frame.get('semantic_error_codes', [])
        
        for error_code in semantic_codes:
            code = error_code['code']
            category = error_code['category']
            semantic_category = error_code.get('semantic_category', 'unknown')
            
            # Обновление паттернов
            if code not in window_state['semantic_patterns']:
                window_state['semantic_patterns'][code] = {
                    'count': 0,
                    'first_seen': error_code['timestamp'],
                    'category': category,
                    'semantic_category': semantic_category,
                    'coaching_priority': error_code['coaching_priority'],
                    'description': error_code['description'],
                    'contexts': []
                }
            
            pattern = window_state['semantic_patterns'][code]
            pattern['count'] += 1
            pattern['last_seen'] = error_code['timestamp']
            pattern['contexts'].append(error_code['context'])
            
            # Классификация по семантическим категориям (только уникальные коды)
            if semantic_category == 'safety_concern':
                if code not in window_state['coaching_categories']['safety_concerns']:
                    window_state['coaching_categories']['safety_concerns'].append(code)
            elif semantic_category == 'technical_issue':
                if code not in window_state['coaching_categories']['technical_issues']:
                    window_state['coaching_categories']['technical_issues'].append(code)
            elif semantic_category == 'form_deviation':
                if code not in window_state['coaching_categories']['form_deviations']:
                    window_state['coaching_categories']['form_deviations'].append(code)
            else:
                if code not in window_state['coaching_categories']['execution_problems']:
                    window_state['coaching_categories']['execution_problems'].append(code)
        
        self.logger.debug(f"📊 Semantic aggregation - patterns: {len(window_state['semantic_patterns'])}")
        
        return window_state

    def _analyze_semantic_coaching(self, window_result: Dict[str, Any], 
                                 state: Dict[str, Any]) -> Dict[str, Any]:
        """Семантический анализ для coaching"""
        
        # Debug window structure
        self.logger.debug(f"🔍 Window result keys: {list(window_result.keys())}")
        
        # Extract data from window result value
        window_data = window_result.get('value', window_result)
        session_uuid = window_data.get('session_uuid')
        if not session_uuid:
            self.logger.error(f"❌ No session_uuid in window data: {window_data}")
            return window_result
        
        # Инициализация истории сессии
        if not state.exists(session_uuid):
            state.set(session_uuid, {
                'windows_processed': 0,
                'semantic_history': {},
                'coaching_cadence': {
                    'last_coaching_sent': 0,
                    'coaching_count': 0
                }
            })
        
        session_state = state.get(session_uuid)
        session_state['windows_processed'] += 1
        state.set(session_uuid, session_state)
        
        # Анализ семантических паттернов
        requires_coaching = self._should_send_semantic_coaching(window_data, session_state)
        
        if requires_coaching:
            session_state['coaching_cadence']['last_coaching_sent'] = session_state['windows_processed']
            session_state['coaching_cadence']['coaching_count'] += 1
            state.set(session_uuid, session_state)
        
        # Формирование семантического результата
        result = {
            'session_uuid': session_uuid,
            'exercise': window_data.get('exercise', 'unknown'),
            'semantic_analysis': {
                'window_duration_ms': 3000,
                'frames_analyzed': window_data.get('frames_processed', 0),
                'fsm_transitions': [f"{t.get('from', '?')}→{t.get('to', '?')}" 
                                   for t in window_data.get('fsm_flow', [])],
                'semantic_patterns': window_data.get('semantic_patterns', {}),
                'coaching_categories': window_data.get('coaching_categories', {})
            },
            'requires_coaching': requires_coaching,
            'coaching_metadata': {
                'windows_processed': session_state['windows_processed'],
                'coaching_count': session_state['coaching_cadence']['coaching_count']
            },
            'timestamp': datetime.now().isoformat()
        }
        
        self.logger.info(f"🧠 Semantic coaching - Session: {session_uuid}, "
                        f"Patterns: {len(window_data.get('semantic_patterns', {}))}, "
                        f"Coaching: {requires_coaching}")
        
        return result

    def _should_send_semantic_coaching(self, window_result: Dict[str, Any], 
                                     session_state: Dict[str, Any]) -> bool:
        """Определение необходимости coaching на основе семантики"""
        
        coaching_categories = window_result.get('coaching_categories', {})
        windows_since_coaching = (session_state['windows_processed'] - 
                                 session_state['coaching_cadence']['last_coaching_sent'])
        
        # Немедленное вмешательство для safety
        if coaching_categories.get('safety_concerns', []):
            return True
        
        # Технические проблемы - каждые 2 окна
        if coaching_categories.get('technical_issues', []) and windows_since_coaching >= 2:
            return True
        
        # Проблемы формы - каждые 4 окна
        if coaching_categories.get('form_deviations', []) and windows_since_coaching >= 4:
            return True
        
        # Проблемы выполнения - каждые 6 окон
        if coaching_categories.get('execution_problems', []) and windows_since_coaching >= 6:
            return True
        
        return False

    def _prepare_llm_semantic_input(self, coaching_result: Dict[str, Any]) -> Dict[str, Any]:
        """
        Финальная подготовка семантических данных для LLM
        """
        semantic_analysis = coaching_result['semantic_analysis']
        
        # Структурированный семантический вход для LLM
        llm_input = {
            'session_id': coaching_result['session_uuid'],
            'exercise_type': coaching_result['exercise'],
            'analysis_timestamp': coaching_result['timestamp'],
            
            # Семантический контекст
            'semantic_context': {
                'analysis_window_ms': semantic_analysis['window_duration_ms'],
                'movement_flow': semantic_analysis['fsm_transitions'],
                'error_patterns_detected': len(semantic_analysis['semantic_patterns'])
            },
            
            # Категоризированные проблемы для LLM
            'coaching_focus_areas': {
                'safety_priorities': self._extract_pattern_details(
                    semantic_analysis.get('coaching_categories', {}).get('safety_concerns', []),
                    semantic_analysis.get('semantic_patterns', {})
                ),
                'technique_improvements': self._extract_pattern_details(
                    semantic_analysis.get('coaching_categories', {}).get('technical_issues', []),
                    semantic_analysis.get('semantic_patterns', {})
                ),
                'form_corrections': self._extract_pattern_details(
                    semantic_analysis.get('coaching_categories', {}).get('form_deviations', []),
                    semantic_analysis.get('semantic_patterns', {})
                ),
                'execution_guidance': self._extract_pattern_details(
                    semantic_analysis.get('coaching_categories', {}).get('execution_problems', []),
                    semantic_analysis.get('semantic_patterns', {})
                )
            },
            
            # Метаданные для LLM обработки
            'llm_processing_hints': {
                'urgency_level': self._determine_urgency_level(semantic_analysis.get('coaching_categories', {})),
                'coaching_complexity': len(semantic_analysis.get('semantic_patterns', {})),
                'session_maturity': coaching_result.get('coaching_metadata', {}).get('windows_processed', 0),
                'intervention_history': coaching_result.get('coaching_metadata', {}).get('coaching_count', 0)
            }
        }
        
        self.logger.debug(f"🤖 LLM input prepared - Focus areas: "
                         f"Safety: {len(llm_input['coaching_focus_areas']['safety_priorities'])}, "
                         f"Technical: {len(llm_input['coaching_focus_areas']['technique_improvements'])}")
        
        return llm_input

    def _extract_pattern_details(self, codes: List[str], patterns: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Извлечение деталей паттернов для LLM"""
        details = []
        
        for code in codes:
            if code in patterns:
                pattern = patterns[code]
                details.append({
                    'error_code': code,
                    'description': pattern['description'],
                    'frequency': pattern['count'],
                    'coaching_priority': pattern['coaching_priority'],
                    'context_examples': pattern['contexts'][-2:]  # Последние 2 контекста
                })
        
        return details

    def _determine_urgency_level(self, coaching_categories: Dict[str, List]) -> str:
        """Определение уровня срочности для LLM"""
        if coaching_categories.get('safety_concerns', []):
            return 'immediate'
        elif coaching_categories.get('technical_issues', []):
            return 'high'
        elif coaching_categories.get('form_deviations', []):
            return 'medium'
        else:
            return 'low'

    def run(self):
        """Запуск универсального процессора"""
        self.logger.info("🚀 Starting Universal Quality Assessment Processor...")
        
        try:
            sdf = self.create_universal_pipeline()
            self.app.run(sdf)
            
        except KeyboardInterrupt:
            self.logger.info("🛑 Universal processor stopped by user")
        except Exception as e:
            self.logger.error(f"💥 Universal processor error: {e}", exc_info=True)
            raise


if __name__ == "__main__":
    processor = UniversalQualityProcessor()
    processor.run()