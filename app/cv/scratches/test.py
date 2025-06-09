def _update_state(self, current_state: Dict, new_state: str) -> Dict:
    """
    Обновление состояния упражнения в соответствии с оригинальной логикой

    Args:
        current_state: Текущее состояние из Redis
        new_state: Новое состояние, определенное на основе углов
    """
    success_sequence = self.state_machine["success_sequence"]

    # Обновляем последовательность состояний
    if new_state in success_sequence:
        if len(current_state['state_seq']) == 0 or new_state != current_state['state_seq'][-1]:
            current_state['state_seq'].append(new_state)

    # Подсчет повторений
    if new_state == success_sequence[0]:  # Возвращение в начальное состояние
        if len(current_state['state_seq']) in [len(success_sequence) - i for i in range(2)]:
            if not current_state['flags']['incorrect_posture'] and not current_state['flags']['critical_error']:
                current_state['curls']['correct'] += 1
            else:
                current_state['curls']['incorrect'] += 1
            # Очищаем state_seq и сбрасываем флаги
            current_state['state_seq'] = []
            current_state['flags']['incorrect_posture'] = False
            current_state['flags']['critical_error'] = False

    # Проверка неактивности
    if current_state['curr_state'] == new_state:
        current_time = time.time()
        current_state['timers']['inactive_time'] += current_time - current_state.get('last_timestamp', current_time)

        if current_state['timers']['inactive_time'] >= self.thresholds['INACTIVE_THRESH']:
            current_state['curls'] = {'correct': 0, 'incorrect': 0}
            current_state['timers']['inactive_time'] = 0.0
    else:
        current_state['timers']['inactive_time'] = 0.0

    # Обновляем состояния
    current_state['prev_state'] = current_state['curr_state']
    current_state['curr_state'] = new_state
    current_state['last_timestamp'] = time.time()

    return current_state
