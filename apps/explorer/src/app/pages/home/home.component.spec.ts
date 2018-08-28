import { async, ComponentFixture, TestBed } from '@angular/core/testing';
import { RouterTestingModule } from '@angular/router/testing';
import { PageContentModule, BlocksListModule, SearchBoxModule } from '@explorer/shared';
import { MatIconModule, MatProgressSpinnerModule } from '@angular/material';
import { LtoPublicNodeService } from '@legalthings-one/platform';
import { NotificationService } from '@legalthings-one/component-kit';
import { of } from 'rxjs';

import { HomeComponent } from './home.component';

describe('HomeComponent', () => {
  let component: HomeComponent;
  let fixture: ComponentFixture<HomeComponent>;
  let publicNodeMock: Partial<LtoPublicNodeService>;
  let notificationsMock: Partial<NotificationService>;

  beforeEach(async(() => {
    publicNodeMock = {
      lastBlocks: () => of([])
    };
    notificationsMock = {};

    TestBed.configureTestingModule({
      imports: [
        RouterTestingModule,
        PageContentModule,
        BlocksListModule,
        MatIconModule,
        MatProgressSpinnerModule,
        SearchBoxModule
      ],
      declarations: [HomeComponent],
      providers: [
        {
          provide: LtoPublicNodeService,
          useValue: publicNodeMock
        },
        {
          provide: NotificationService,
          useValue: notificationsMock
        }
      ]
    }).compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(HomeComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
