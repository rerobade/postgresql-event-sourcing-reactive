package eventsourcing.postgresql.service;

import eventsourcing.postgresql.domain.Aggregate;
import eventsourcing.postgresql.domain.command.Command;
import eventsourcing.postgresql.service.command.CommandHandler;
import eventsourcing.postgresql.service.command.DefaultCommandHandler;
import eventsourcing.postgresql.service.event.SyncEventHandler;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@Transactional
@Component
@RequiredArgsConstructor
@Slf4j
public class CommandProcessor {

    private final AggregateStore aggregateStore;
    private final List<CommandHandler<? extends Command>> commandHandlers;
    private final DefaultCommandHandler defaultCommandHandler;
    private final List<SyncEventHandler> aggregateChangesHandlers;

    public Mono<Aggregate> process(@NonNull Command command) {
        log.debug("Processing command {}", command);

        String aggregateType = command.getAggregateType();
        UUID aggregateId = command.getAggregateId();

        return aggregateStore.readAggregate(aggregateType, aggregateId)
                .flatMap(aggregate -> {
                    commandHandlers.stream()
                            .filter(commandHandler -> commandHandler.getCommandType() == command.getClass())
                            .findFirst()
                            .ifPresentOrElse(commandHandler -> {
                                log.debug("Handling command {} with {}", command.getClass().getSimpleName(), commandHandler.getClass().getSimpleName());
                                commandHandler.handle(aggregate, command);
                            }, () -> {
                                log.debug("No specialized handler found, handling command {} with {}", command.getClass().getSimpleName(), defaultCommandHandler.getClass().getSimpleName());
                                defaultCommandHandler.handle(aggregate, command);
                            });
                    return aggregateStore.saveAggregate(aggregate)
                            .collectList()
                            .flatMapMany(newEvents -> {
                                List<Mono<Void>> storedChanges = new ArrayList<>();
                                aggregateChangesHandlers.stream()
                                        .filter(handler -> handler.getAggregateType().equals(aggregateType))
                                        .forEach(handler -> {
                                            storedChanges.add(handler.handleEvents(newEvents, aggregate));
                                });
                                return Flux.merge(storedChanges);
                            }).collectList()
                            .thenReturn(aggregate);
                });
    }
}
